package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"log/slog"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	KafkaTopic     = "raw_logs"
	MaxMessageSize = 64 * 1024 // max message size 64 kb
)

var KafkaBrokers = os.Getenv("KAFKA_BROKERS")

type LogEntry struct {
	Timestamp string            `json:"timestamp" binding:"required"`
	Level     string            `json:"level" binding:"required"`
	Message   string            `json:"message" binding:"required"`
	Service   string            `json:"service,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type ValidationResult struct {
	IsValid bool              `json:"is_valid"`
	Errors  []ValidationError `json:"errors,omitempty"`
}

type LogValidator struct {
	validLevels    map[string]bool
	timestampRegex *regexp.Regexp
}

func NewLogValidator() *LogValidator {
	validLevels := map[string]bool{
		"DEBUG":   true,
		"INFO":    true,
		"WARN":    true,
		"WARNING": true,
		"ERROR":   true,
		"FATAL":   true,
		"PANIC":   true,
	}

	timestampRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$`)

	return &LogValidator{
		validLevels:    validLevels,
		timestampRegex: timestampRegex,
	}
}

func (v *LogValidator) ValidateSize(data []byte) *ValidationError {
	if len(data) == 0 {
		return &ValidationError{
			Field:   "body",
			Message: "log message cannot be empty",
		}
	}

	if len(data) > MaxMessageSize {
		return &ValidationError{
			Field:   "body",
			Message: fmt.Sprintf("message size exceeds maximum allowed size of %d bytes", MaxMessageSize),
		}
	}

	return nil
}

func (v *LogValidator) ValidateJSON(data []byte) (*LogEntry, *ValidationError) {
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, &ValidationError{
			Field:   "json",
			Message: "invalid JSON format",
		}
	}
	return &entry, nil
}

func (v *LogValidator) ValidateLogEntry(entry *LogEntry) []ValidationError {
	var errors []ValidationError

	if entry.Timestamp == "" {
		errors = append(errors, ValidationError{
			Field:   "timestamp",
			Message: "timestamp is required",
		})
	} else {
		if _, err := time.Parse(time.RFC3339, entry.Timestamp); err != nil {
			if !v.timestampRegex.MatchString(entry.Timestamp) {
				errors = append(errors, ValidationError{
					Field:   "timestamp",
					Message: "timestamp must be in RFC3339 or ISO 8601 format",
				})
			}
		}
	}

	if entry.Level == "" {
		errors = append(errors, ValidationError{
			Field:   "level",
			Message: "log level is required",
		})
	} else {
		upperLevel := strings.ToUpper(entry.Level)
		if !v.validLevels[upperLevel] {
			errors = append(errors, ValidationError{
				Field:   "level",
				Message: "invalid log level. Must be one of: DEBUG, INFO, WARN, WARNING, ERROR, FATAL, PANIC",
			})
		}
	}

	if entry.Message == "" {
		errors = append(errors, ValidationError{
			Field:   "message",
			Message: "log message cannot be empty",
		})
	} else if len(entry.Message) > 10000 {
		errors = append(errors, ValidationError{
			Field:   "message",
			Message: "log message exceeds maximum length of 10000 characters",
		})
	}

	if entry.Service != "" {
		serviceRegex := regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
		if !serviceRegex.MatchString(entry.Service) {
			errors = append(errors, ValidationError{
				Field:   "service",
				Message: "service name can only letters, numbers, hypens, and underscores",
			})
		}
	}

	return errors
}

func (v *LogValidator) ValidateComplete(data []byte) ValidationResult {
	var allErrors []ValidationError

	if sizeErr := v.ValidateSize(data); sizeErr != nil {
		return ValidationResult{
			IsValid: false,
			Errors:  []ValidationError{*sizeErr},
		}
	}

	logEntry, jsonErr := v.ValidateJSON(data)
	if jsonErr != nil {
		return ValidationResult{
			IsValid: false,
			Errors:  []ValidationError{*jsonErr},
		}
	}

	contentErrors := v.ValidateLogEntry(logEntry)
	allErrors = append(allErrors, contentErrors...)

	return ValidationResult{
		IsValid: len(allErrors) == 0,
		Errors:  allErrors,
	}
}

func main() {
	// Initialize a structured logger that outputs JSON to standard output.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0

	KafkaBrokers := os.Getenv("KAFKA_BROKERS")
	brokers := strings.Split(KafkaBrokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		logger.Error("Failed to start Sarama producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	validator := NewLogValidator()
	router := gin.Default()

	// Replace Gin's default logger with our structured logger or disable it if not needed.
	// For simplicity, we'll keep Gin's logger for request logging, but our app logs will be structured.
	// router.Use(gin.Logger())

	router.POST("/log", func(c *gin.Context) {
		body, err := c.GetRawData()
		if err != nil {
			logger.Error("Failed to read request body", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body",
				"details": err.Error(),
			})
			return
		}

		validationResult := validator.ValidateComplete(body)

		if !validationResult.IsValid {
			logger.Info("Log validation failed", "validation_errors", validationResult.Errors)
			c.JSON(http.StatusBadRequest, gin.H{
				"error":             "validation failed",
				"validation_errors": validationResult.Errors,
			})
			return
		}

		msg := &sarama.ProducerMessage{
			Topic: KafkaTopic,
			Value: sarama.ByteEncoder(body),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			logger.Error("Failed to send message to Kafka", "error", err, "topic", KafkaTopic)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "failed to send message to kafka",
				"details": err.Error(),
			})
			return
		}

		logger.Info("Log received and validated successfully", "topic", KafkaTopic, "partition", partition, "offset", offset)
		c.JSON(http.StatusOK, gin.H{
			"message":   "log recieved and validated successfully",
			"partition": partition,
			"offset":    offset,
		})

	})

	router.GET("/validation-rules", func(c *gin.Context) {
		logger.Info("Validation rules requested")
		c.JSON(http.StatusOK, gin.H{
			"max_message_size": MaxMessageSize,
			"required_fields":  []string{"timestamp", "level", "message"},
			"valid_log_levels": []string{"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL", "PANIC"},
			"timestamp_format": "RFC3339 or ISO 8601",
		})
	})

	logger.Info("Ingestor service starting", "port", 8081)
	if err := router.Run(":8081"); err != nil {
		logger.Error("Failed to run Gin server", "error", err)
		os.Exit(1)
	}
}

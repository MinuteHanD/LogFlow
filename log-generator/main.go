package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// These are the fake services that will be generating logs.
var (
	services = []string{"api-gateway", "user-service", "payment-processor", "auth-service", "notification-sender"}
	levels   = []string{"INFO", "WARN", "ERROR", "DEBUG"}
)

// LogEntry defines the structure of the log we're sending.
// It matches the structure your ingestor expects.
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Service   string `json:"service"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Log generator service starting up...")

	ingestorURL := os.Getenv("INGESTOR_URL")
	if ingestorURL == "" {
		logger.Error("INGESTOR_URL environment variable is not set. Can't send logs.")
		os.Exit(1)
	}
	ingestorHealthURL := strings.Replace(ingestorURL, "/log", "/health", 1)

	// Wait for the ingestor to be ready
	for {
		resp, err := http.Get(ingestorHealthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			logger.Info("Ingestor service is ready.")
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		logger.Info("Waiting for ingestor service to be ready...")
		time.Sleep(2 * time.Second)
	}

	logger.Info("Logs will be sent to", "url", ingestorURL)

	// Seed the random number generator
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	// Infinite loop to continuously generate logs
	for {
		// Sleep for a random duration between 500ms and 3s to simulate real traffic
		sleepDuration := time.Duration(500+random.Intn(2500)) * time.Millisecond
		time.Sleep(sleepDuration)

		// Create a new log entry with random data
		logEntry := generateRandomLog(random)

		// Convert the log entry to JSON
		payload, err := json.Marshal(logEntry)
		if err != nil {
			// This should ideally never happen with our struct
			logger.Error("Failed to marshal log entry to JSON", "error", err)
			continue
		}

		// Send the log to the ingestor service
		resp, err := http.Post(ingestorURL, "application/json", bytes.NewReader(payload))
		if err != nil {
			logger.Error("Failed to send log to ingestor", "error", err, "service", logEntry.Service)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			logger.Warn("Ingestor returned a non-OK status", "status_code", resp.StatusCode, "service", logEntry.Service)
		} else {
			logger.Info("Successfully sent a log", "service", logEntry.Service, "level", logEntry.Level)
		}
		resp.Body.Close()
	}
}

// generateRandomLog creates a single log entry with randomized content.
func generateRandomLog(random *rand.Rand) LogEntry {
	service := services[random.Intn(len(services))]
	level := levels[random.Intn(len(levels))]
	message := generateRandomMessage(service, level, random)

	return LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     level,
		Message:   message,
		Service:   service,
	}
}

// generateRandomMessage creates a more realistic log message based on service and level.
func generateRandomMessage(service, level string, random *rand.Rand) string {
	userID := 1000 + random.Intn(9000)
	switch level {
	case "ERROR":
		errorMessages := []string{
			"Failed to connect to database: timeout expired",
			"Payment failed for user %d: insufficient funds",
			"Authentication token expired for user %d",
			"Null pointer exception at process step %d",
		}
		return fmt.Sprintf(errorMessages[random.Intn(len(errorMessages))], userID)
	case "WARN":
		warnMessages := []string{
			"High latency detected on upstream service: %dms",
			"Disk space running low: %d%% remaining",
			"API rate limit approaching for client %d",
		}
		return fmt.Sprintf(warnMessages[random.Intn(len(warnMessages))], 50+random.Intn(200))
	default: // INFO or DEBUG
		infoMessages := []string{
			"User %d logged in successfully.",
			"Processed new order #%d for user %d.",
			"Retrieved user profile for user %d.",
		}
		return fmt.Sprintf(infoMessages[random.Intn(len(infoMessages))], userID, 100+random.Intn(500), userID)
	}
}

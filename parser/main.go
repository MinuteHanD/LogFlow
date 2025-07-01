package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"log/slog"

	"github.com/IBM/sarama"
)

const (
	InputTopic    = "raw_logs"
	OutputTopic   = "parsed_logs"
	DLQTopic      = "raw_logs_dlq"
	ConsumerGroup = "parser-group"
)

type IncomingLogEntry struct {
	Timestamp string            `json:"timestamp"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Service   string            `json:"service,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type ParsedLog struct {
	ID               string            `json:"id"`
	Timestamp        time.Time         `json:"timestamp"`
	TimestampMs      int64             `json:"timestamp_ms"`
	Level            string            `json:"level"`
	Service          string            `json:"service"`
	Message          string            `json:"message"`
	MessageHash      string            `json:"message_hash"`
	Metadata         map[string]string `json:"metadata"`
	ProcessedAt      string            `json:"processed_at"`
	ProcessorVersion string            `json:"processor_version"`
}

type LogProcessor struct {
	version string
	logger  *slog.Logger
}

func NewLogProcessor(logger *slog.Logger) *LogProcessor {
	return &LogProcessor{
		version: "1.0.0",
		logger:  logger,
	}
}

func (p *LogProcessor) ProcessLog(raw IncomingLogEntry) (*ParsedLog, error) {
	id := generateSimpleID()

	parsedTime, timestampMs, err := p.normalizeTimestamp(raw.Timestamp)
	if err != nil {
		return nil, err
	}

	normalizedLevel := p.normalizeLogLevel(raw.Level)

	service := raw.Service
	if service == "" {
		service = "unknown"
	}

	metadata := raw.Metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}

	metadata["original_level"] = raw.Level
	if raw.Service == "" {
		metadata["service_defaulted"] = "true"
	}

	messageHash := generateMessageHash(raw.Message)

	return &ParsedLog{
		ID:               id,
		Timestamp:        parsedTime,
		TimestampMs:      timestampMs,
		Level:            normalizedLevel,
		Service:          service,
		Message:          raw.Message,
		MessageHash:      messageHash,
		Metadata:         metadata,
		ProcessedAt:      time.Now().UTC().Format(time.RFC3339),
		ProcessorVersion: p.version,
	}, nil
}

func (p *LogProcessor) normalizeTimestamp(timestamp string) (time.Time, int64, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
	}

	var parsedTime time.Time
	var err error

	for _, format := range formats {
		parsedTime, err = time.Parse(format, timestamp)
		if err == nil {

			break
		}
	}

	if err != nil {

		return time.Time{}, 0, fmt.Errorf("could not parse timestamp: %s", timestamp)
	}

	utcTime := parsedTime.UTC()
	return utcTime, utcTime.UnixMilli(), nil
}

func (p *LogProcessor) normalizeLogLevel(level string) string {
	upperLevel := strings.ToUpper(level)

	levelMap := map[string]string{
		"DEBUG":   "DEBUG",
		"INFO":    "INFO",
		"WARN":    "WARN",
		"WARNING": "WARN",
		"ERROR":   "ERROR",
		"FATAL":   "FATAL",
		"PANIC":   "FATAL",
	}
	if normalized, exists := levelMap[upperLevel]; exists {
		return normalized
	}

	p.logger.Warn("Unknown log level, defaulting to INFO", "level", level)
	return "INFO"
}

func generateSimpleID() string {
	return strings.ReplaceAll(time.Now().Format("20060102-150405.000000"), ".", "-")
}

func generateMessageHash(message string) string {
	hash := 0
	for _, char := range message {
		hash = hash*31 + int(char)
	}
	return string(rune(hash))
}

func sendToDLQ(logger *slog.Logger, producer sarama.SyncProducer, originalMessage *sarama.ConsumerMessage, processingError error) {
	dlqMessage := &sarama.ProducerMessage{
		Topic: DLQTopic,
		Value: sarama.ByteEncoder(originalMessage.Value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("error"),
				Value: []byte(processingError.Error()),
			},
			{
				Key:   []byte("original_topic"),
				Value: []byte(originalMessage.Topic),
			},
			{
				Key:   []byte("original_partition"),
				Value: []byte(fmt.Sprintf("%d", originalMessage.Partition)),
			},
			{
				Key:   []byte("original_offset"),
				Value: []byte(fmt.Sprintf("%d", originalMessage.Offset)),
			},
		},
	}

	_, _, err := producer.SendMessage(dlqMessage)
	if err != nil {
		logger.Error("CRITICAL: Failed to send message to DLQ", "topic", DLQTopic, "error", err)
	} else {
		logger.Info("Message sent to DLQ", "topic", DLQTopic, "reason", processingError.Error())
	}
}

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	logger.Info("Starting parser service...")

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		logger.Error("KAFKA_BROKERS environment variable not set")
		os.Exit(1)
	}
	brokers := strings.Split(kafkaBrokers, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, ConsumerGroup, config)
	if err != nil {
		logger.Error("Failed to create consumer group", "error", err)
		os.Exit(1)
	}
	defer consumerGroup.Close()

	producer, err := newProducer(brokers)
	if err != nil {
		logger.Error("Failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())

	processor := NewLogProcessor(logger)

	handler := &logHandler{
		producer:  producer,
		processor: processor,
		ready:     make(chan bool),
		logger:    logger,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{InputTopic}, handler); err != nil {
				logger.Error("Error from consumer", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	<-handler.ready
	logger.Info("Parser consumer is up and running!")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	logger.Info("Termination signal received. Shutting down")
	cancel()
	wg.Wait()
}

func newProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0
	return sarama.NewSyncProducer(brokers, config)
}

type logHandler struct {
	producer  sarama.SyncProducer
	processor *LogProcessor
	ready     chan bool
	logger    *slog.Logger
}

func (h *logHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(h.ready) //unblock
	return nil
}

func (h *logHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *logHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.logger.Info("Received message", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)

		var incomingLog IncomingLogEntry
		if err := json.Unmarshal(message.Value, &incomingLog); err != nil {
			h.logger.Error("Failed to unmarshal incoming log", "error", err, "topic", message.Topic, "offset", message.Offset)

			sendToDLQ(h.logger, h.producer, message, err)

			session.MarkMessage(message, "")
			continue
		}

		parsedLog, err := h.processor.ProcessLog(incomingLog)
		if err != nil {
			h.logger.Error("Failed to process log", "error", err, "topic", message.Topic, "offset", message.Offset)

			sendToDLQ(h.logger, h.producer, message, err)

			session.MarkMessage(message, "")
			continue
		}

		enrichedLogBytes, err := json.Marshal(parsedLog)
		if err != nil {
			h.logger.Error("Failed to marshal processed log", "error", err, "log_id", parsedLog.ID)
			session.MarkMessage(message, "")
			continue
		}

		producerMsg := &sarama.ProducerMessage{
			Topic: OutputTopic,
			Value: sarama.ByteEncoder(enrichedLogBytes),
		}

		partition, offset, err := h.producer.SendMessage(producerMsg)
		if err != nil {
			h.logger.Error("Failed to send processed log to Kafka", "error", err, "topic", OutputTopic, "log_id", parsedLog.ID)
		} else {
			h.logger.Info("Successfully sent processed log to Kafka", "topic", OutputTopic, "partition", partition, "offset", offset, "log_id", parsedLog.ID)
		}

		session.MarkMessage(message, "")
	}

	return nil
}

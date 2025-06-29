package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	InputTopic    = "raw_logs"
	OutputTopic   = "parsed_logs"
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
}

func NewLogProcessor() *LogProcessor {
	return &LogProcessor{
		version: "1.0.0",
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
	if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
		return t.UTC(), t.UnixMilli(), nil
	}

	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timestamp); err == nil {
			return t.UTC(), t.UnixMilli(), nil
		}
	}

	log.Printf("Warning: Could not parse timestamp %s, defaulting to current time", timestamp)
	now := time.Now().UTC()
	return now, now.UnixMilli(), nil
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

	log.Printf("Warning: Unknown log level %s, defaulting to INFO", level)
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

func main() {
	log.Println("Starting parser service...")

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal("KAFKA_BROKERS environment variable not set")
	}
	brokers := strings.Split(kafkaBrokers, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, ConsumerGroup, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	producer, err := newProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())

	processor := NewLogProcessor()

	handler := &logHandler{
		producer:  producer,
		processor: processor,
		ready:     make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{InputTopic}, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	<-handler.ready
	log.Println("Parser consumer is up and running!")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Termination signal received. Shutting down")
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
		log.Printf("Received message on topic %s", message.Topic)

		var incomingLog IncomingLogEntry
		if err := json.Unmarshal(message.Value, &incomingLog); err != nil {
			log.Printf("Failed to unmarshal incoming log: %v", err)
			log.Printf("Raw message; %s", string(message.Value))
			session.MarkMessage(message, "")
			continue
		}

		parsedLog, err := h.processor.ProcessLog(incomingLog)
		if err != nil {
			log.Printf("Failed to process log: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		enrichedLogBytes, err := json.Marshal(parsedLog)
		if err != nil {
			log.Printf("Failed to marshal processed log: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		producerMsg := &sarama.ProducerMessage{
			Topic: OutputTopic,
			Value: sarama.ByteEncoder(enrichedLogBytes),
		}

		partition, offset, err := h.producer.SendMessage(producerMsg)
		if err != nil {
			log.Printf("Failed to send processed log to kafka: %v", err)
		} else {
			log.Printf("Successfully sent processed log to topic %s (partition: %d, offset: %d)",
				OutputTopic, partition, offset)
		}

		session.MarkMessage(message, "")
	}

	return nil
}

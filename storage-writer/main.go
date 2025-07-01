package main

import (
	"bytes"
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
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

const (
	InputTopic            = "parsed_logs"
	ConsumerGroup         = "storage-writer-group"
	ElasticsearchDLQTopic = "parsed_logs_dlq" // Dead-Letter Queue topic for Elasticsearch failures
	IndexPrefix           = "logs"
)

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

type ElasticsearchDocument struct {
	ParsedLog
	IndexedAt      string `json:"indexed_at"`
	IndexName      string `json:"index_name"`
	StorageVersion string `json:"storage_version"`
}

type IndexManager struct {
	client *elasticsearch.Client
	logger *slog.Logger
}

func NewIndexManager(client *elasticsearch.Client, logger *slog.Logger) *IndexManager {
	return &IndexManager{
		client: client,
		logger: logger,
	}
}

func (im *IndexManager) CreateIndexIfNotExists(indexName string) error {

	req := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(context.Background(), im.client)
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		im.logger.Info("Index already exists", "index_name", indexName)
		return nil
	}

	mapping := `{
		"mappings": {
			"properties": {
				"id": { "type": "keyword" },
				"timestamp": { "type": "date"},
				"timestamp_ms": { "type": "long" },
				"level": { "type": "keyword" },
				"service": { "type": "keyword" },
				"message": { "type": "text", "analyzer": "standard" },
				"message_hash": { "type": "keyword" },
				"metadata": { "type": "object", "dynamic": "true" },
				"processed_at": { "type": "date"},
				"processor_version": { "type": "keyword" },
				"indexed_at": { "type": "date"},
				"index_name": { "type": "keyword" },
				"storage_version": { "type": "keyword" }
			}
		},
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 1,
			"refresh_interval": "1s"
		}	
	}`

	reqCreate := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}

	createRes, err := reqCreate.Do(context.Background(), im.client)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer createRes.Body.Close()

	if createRes.IsError() {
		return fmt.Errorf("failed to create index: %s", createRes.String())
	}

	im.logger.Info("Successfully created index", "index_name", indexName)
	return nil
}

func (im *IndexManager) GetIndexname(timestamp time.Time) string {
	t := timestamp
	if t.IsZero() {
		im.logger.Warn("Invalid timestamp (zero value), using current time for index name")
		t = time.Now()
	}
	return fmt.Sprintf("%s-%s", IndexPrefix, t.Format("2006.01.02"))
}

type LogStorageProcessor struct {
	esClient     *elasticsearch.Client
	indexManager *IndexManager
	version      string
	logger       *slog.Logger
}

func NewLogStorageProcessor(client *elasticsearch.Client, logger *slog.Logger) *LogStorageProcessor {
	return &LogStorageProcessor{
		esClient:     client,
		indexManager: NewIndexManager(client, logger),
		version:      "1.0.0",
		logger:       logger,
	}
}

func (lsp *LogStorageProcessor) ProcessAndStore(messageValue []byte) error {
	var parsedLog ParsedLog
	if err := json.Unmarshal(messageValue, &parsedLog); err != nil {
		return fmt.Errorf("failed to unmarshal log: %w", err)
	}

	indexName := lsp.indexManager.GetIndexname(parsedLog.Timestamp)

	if err := lsp.indexManager.CreateIndexIfNotExists(indexName); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	doc := ElasticsearchDocument{
		ParsedLog:      parsedLog,
		IndexedAt:      time.Now().UTC().Format(time.RFC3339),
		IndexName:      indexName,
		StorageVersion: lsp.version,
	}

	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: parsedLog.ID,
		Body:       bytes.NewReader(docBytes),
		Refresh:    "false",
	}

	res, err := req.Do(context.Background(), lsp.esClient)
	if err != nil {
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch indexing error: %s", res.String())
	}

	lsp.logger.Info("Successfully indexed document", "document_id", parsedLog.ID, "index_name", indexName)
	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	logger.Info("Starting storage writer...")

	kafkaEnv := os.Getenv("KAFKA_BROKERS")
	esEnv := os.Getenv("ELASTICSEARCH_URL")
	if kafkaEnv == "" || esEnv == "" {
		logger.Error("Environment variables KAFKA_BROKERS and ELASTICSEARCH_URL must be set")
		os.Exit(1)
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esEnv},
	})
	if err != nil {
		logger.Error("Error creating Elasticsearch client", "error", err)
		os.Exit(1)
	}

	res, err := esClient.Info()
	if err != nil {
		logger.Error("Error getting Elasticsearch info", "error", err)
		os.Exit(1)
	}
	defer res.Body.Close()
	if res.IsError() {
		logger.Error("Error response from Elasticsearch", "response", res.String())
		os.Exit(1)
	}
	logger.Info("Connected to Elasticsearch successfully")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := strings.Split(kafkaEnv, ",")

	var consumerGroup sarama.ConsumerGroup
	for i := 0; i < 10; i++ {
		consumerGroup, err = sarama.NewConsumerGroup(brokers, ConsumerGroup, config)
		if err == nil {
			break
		}
		logger.Warn("Kafka not ready, retrying in 5s...", "error", err, "attempt", i+1)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		logger.Error("Failed to create consumer group after retries", "error", err)
		os.Exit(1)
	}
	defer consumerGroup.Close()

	producer, err := newProducer(brokers)
	if err != nil {
		logger.Error("Failed to create producer for DLQ", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := NewLogStorageProcessor(esClient, logger)

	handler := &logStorageHandler{
		processor: processor,
		producer:  producer,
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
	logger.Info("Storage Writer is online! Waiting for messages...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	logger.Info("Termination signal received. Shutting down the service...")
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

func sendToDLQ(logger *slog.Logger, producer sarama.SyncProducer, originalMessage *sarama.ConsumerMessage, processingError error) {
	dlqMessage := &sarama.ProducerMessage{
		Topic: ElasticsearchDLQTopic,
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
		logger.Error("CRITICAL: Failed to send message to Elasticsearch DLQ", "topic", ElasticsearchDLQTopic, "error", err)
	} else {
		logger.Info("Message sent to Elasticsearch DLQ", "topic", ElasticsearchDLQTopic, "reason", processingError.Error())
	}
}

type logStorageHandler struct {
	processor *LogStorageProcessor
	producer  sarama.SyncProducer
	ready     chan bool
	logger    *slog.Logger
}

func (handler *logStorageHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(handler.ready)
	return nil
}

func (handler *logStorageHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (handler *logStorageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		handler.logger.Info("Received parsed log", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)

		if err := handler.processor.ProcessAndStore(message.Value); err != nil {
			handler.logger.Error("Failed to process and store log", "error", err, "topic", message.Topic, "offset", message.Offset)

			// Send the failed message to the Dead-Letter Queue
			sendToDLQ(handler.logger, handler.producer, message, err)

			session.MarkMessage(message, "")
			continue
		}

		session.MarkMessage(message, "")
	}

	return nil
}

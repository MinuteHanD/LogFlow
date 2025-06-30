package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

const (
	InputTopic          = "parsed_logs"
	ConsumerGroup       = "storage-writer-group"
	ElasticsearchDLQTopic = "parsed_logs_dlq" // Dead-Letter Queue topic for Elasticsearch failures
	IndexPrefix         = "logs"
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
}

func NewIndexManager(client *elasticsearch.Client) *IndexManager {
	return &IndexManager{
		client: client,
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
		log.Printf("Index %s already exists", indexName)
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

	log.Printf("Successfully created index %s", indexName)
	return nil
}

func (im *IndexManager) GetIndexname(timestamp time.Time) string {
	t := timestamp
	if t.IsZero() {
		log.Printf("Invalid timestamp (zero value), using current time")
		t = time.Now()
	}
	return fmt.Sprintf("%s-%s", IndexPrefix, t.Format("2006.01.02"))
}

type LogStorageProcessor struct {
	esClient     *elasticsearch.Client
	indexManager *IndexManager
	version      string
}

func NewLogStorageProcessor(client *elasticsearch.Client) *LogStorageProcessor {
	return &LogStorageProcessor{
		esClient:     client,
		indexManager: NewIndexManager(client),
		version:      "1.0.0",
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

	log.Printf("Successfully indexed document %s to %s", parsedLog.ID, indexName)
	return nil
}

func main() {
	log.Println("Starting storage writer...")

	kafkaEnv := os.Getenv("KAFKA_BROKERS")
	esEnv := os.Getenv("ELASTICSEARCH_URL")
	if kafkaEnv == "" || esEnv == "" {
		log.Fatalf("Environment variables KAFKA_BROKERS and ELASTICSEARCH_URL must be set")
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esEnv},
	})
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	res, err := esClient.Info()
	if err != nil {
		log.Fatalf("Error getting Elasticsearch info: %v", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Fatalf("Error response from Elasticsearch: %s", res.String())
	}
	log.Println("Connected to Elasticsearch successfully")

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
		log.Printf("Kafka not ready (%v), retrying in 5s...", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Fatalf("Failed to create consumer group after retries: %v", err)
	}
	defer consumerGroup.Close()

	// Create a producer for sending messages to the DLQ
	producer, err := newProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to create producer for DLQ: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := NewLogStorageProcessor(esClient)

	handler := &logStorageHandler{
		processor: processor,
		producer:  producer, // Pass the producer to the handler
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
	log.Println("Storage Writer is online! Waiting for messages...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Termination signal received. Shutting down the service...")
	cancel()
	wg.Wait()
}

// newProducer creates a new Sarama SyncProducer for sending messages.
func newProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0
	return sarama.NewSyncProducer(brokers, config)
}

// sendToDLQ sends a message to the Dead-Letter Queue for Elasticsearch failures.
// It adds metadata about the failure, such as the error message and the original topic.
func sendToDLQ(producer sarama.SyncProducer, originalMessage *sarama.ConsumerMessage, processingError error) {
	dlqMessage := &sarama.ProducerMessage{
		Topic: ElasticsearchDLQTopic,
		Value: sarama.ByteEncoder(originalMessage.Value), // The original message content
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
		log.Printf("CRITICAL: Failed to send message to Elasticsearch DLQ. Topic: %s. Error: %v", ElasticsearchDLQTopic, err)
	} else {
		log.Printf("Message sent to Elasticsearch DLQ topic %s due to error: %v", ElasticsearchDLQTopic, processingError)
	}
}

type logStorageHandler struct {
	processor *LogStorageProcessor
	producer  sarama.SyncProducer // Add producer to handler
	ready     chan bool
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
		log.Printf("Received parsed log from topic %s", message.Topic)

		if err := handler.processor.ProcessAndStore(message.Value); err != nil {
			log.Printf("Failed to process and store log: %v", err)

			// Send the failed message to the Dead-Letter Queue
			sendToDLQ(handler.producer, message, err)

			// Mark the original message as processed so we don't try it again
			session.MarkMessage(message, "")
			continue
		}

		// If processing and storing was successful, mark the message as processed
		session.MarkMessage(message, "")
	}

	return nil
}

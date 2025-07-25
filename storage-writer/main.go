package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	"github.com/MinuteHanD/log-pipeline/config"
	"github.com/MinuteHanD/log-pipeline/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	ConsumerGroup = "storage-writer-group"
)

var (
	logsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "storage_writer_logs_received_total",
			Help: "Total number of logs received from the parsed_logs topic.",
		},
	)
	logsStored = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "storage_writer_logs_stored_total",
			Help: "Total number of logs successfully stored in Elasticsearch.",
		},
	)
	logsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "storage_writer_logs_failed_total",
			Help: "Total number of logs that failed to be stored in Elasticsearch.",
		},
	)
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
	client      *elasticsearch.Client
	logger      *slog.Logger
	indexPrefix string
}

func NewIndexManager(client *elasticsearch.Client, logger *slog.Logger, indexPrefix string) *IndexManager {
	return &IndexManager{
		client:      client,
		logger:      logger,
		indexPrefix: indexPrefix,
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
	return fmt.Sprintf("%s-%s", im.indexPrefix, t.Format("2006.01.02"))
}

type LogStorageProcessor struct {
	esClient     *elasticsearch.Client
	indexManager *IndexManager
	version      string
	logger       *slog.Logger
}

func NewLogStorageProcessor(client *elasticsearch.Client, logger *slog.Logger, version string, indexPrefix string) *LogStorageProcessor {
	return &LogStorageProcessor{
		esClient:     client,
		indexManager: NewIndexManager(client, logger, indexPrefix),
		version:      version,
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

func init() {
	prometheus.MustRegister(logsReceived)
	prometheus.MustRegister(logsStored)
	prometheus.MustRegister(logsFailed)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("Starting metrics server on port 8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			logger.Error("Failed to start metrics server", "error", err)
		}
	}()

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.Elasticsearch.URL},
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

	var consumerGroup sarama.ConsumerGroup
	for i := 0; i < 10; i++ {
		consumerGroup, err = sarama.NewConsumerGroup(cfg.Kafka.Brokers, ConsumerGroup, config)
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

	producer, err := newProducer(cfg.Kafka.Brokers)
	if err != nil {
		logger.Error("Failed to create producer for DLQ", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := NewLogStorageProcessor(esClient, logger, cfg.StorageWriter.Version, cfg.StorageWriter.IndexPrefix)

	handler := &logStorageHandler{
		processor: processor,
		producer:  producer,
		ready:     make(chan bool),
		logger:    logger,
		cfg:       cfg,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{cfg.Kafka.Topics.Parsed}, handler); err != nil {
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



type logStorageHandler struct {
	processor *LogStorageProcessor
	producer  sarama.SyncProducer
	ready     chan bool
	logger    *slog.Logger
	cfg       *config.Config
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
		logsReceived.Inc()
		handler.logger.Info("Received parsed log", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)

		if err := handler.processor.ProcessAndStore(message.Value); err != nil {
			logsFailed.Inc()
			handler.logger.Error("Failed to process and store log", "error", err, "topic", message.Topic, "offset", message.Offset)

			// Send the failed message to the Dead-Letter Queue
			kafka.SendToDLQ(handler.logger, handler.producer, handler.cfg.Kafka.Topics.ParsedDLQ, message, err)

			session.MarkMessage(message, "")
			continue
		}

		logsStored.Inc()
		session.MarkMessage(message, "")
	}

	return nil
}

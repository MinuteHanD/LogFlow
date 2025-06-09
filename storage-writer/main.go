package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
)

const (
	InputTopic    = "parsed_logs"
	ConsumerGroup = "storage-writer-group"
	IndexName     = "logs"
)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &logStorageHandler{
		esClient: esClient,
		ready:    make(chan bool),
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
	log.Println("Storage Writer is running! Waiting for messages...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Termination signal received. Shutting down gracefully...")
	cancel()
	wg.Wait()
}

type logStorageHandler struct {
	esClient *elasticsearch.Client
	ready    chan bool
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

		res, err := handler.esClient.Index(
			IndexName,
			bytes.NewReader(message.Value),
			handler.esClient.Index.WithContext(context.Background()),
		)
		if err != nil {
			log.Printf("Error sending to Elasticsearch: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		if res.IsError() {
			log.Printf("Elasticsearch indexing error: %s", res.String())
		} else {
			log.Printf("Successfully indexed document to '%s'", IndexName)
		}
		res.Body.Close()

		session.MarkMessage(message, "")
	}
	return nil
}

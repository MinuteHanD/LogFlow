package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
)

const (
	KafkaBrokers  = "localhost:9092"
	InputTopic    = "parsed_logs"
	ConsumerGroup = "storage-writer-group"
	ESAddress     = "http://localhost:9200"
)

func main() {
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{ESAddress},
	})
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}
	log.Println("Successfully connected to Elasticsearch")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_1_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaBrokers}, ConsumerGroup, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
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
	log.Println("Storage Writer is up and running! Waiting for parsed logs...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	log.Println("terminating: via signal")
	cancel()
	wg.Wait()
}

type logStorageHandler struct {
	esClient *elasticsearch.Client
	ready    chan bool
}

func (handler *logStorageHandler) Setup(sarama.ConsumerGroupSession) error {
	close(handler.ready)
	return nil
}

func (handler *logStorageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (handler *logStorageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Received parsed log from topic %s", message.Topic)

		res, err := handler.esClient.Index(
			"logs",
			bytes.NewReader(message.Value),
			handler.esClient.Index.WithContext(context.Background()),
		)
		if err != nil {
			log.Printf("Error getting response from Elasticsearch: %s", err)
			continue
		}
		defer res.Body.Close()

		if res.IsError() {
			log.Printf("Error indexing document: %s", res.String())
		} else {
			log.Printf("Successfully indexed document to 'logs' index")
		}

		session.MarkMessage(message, "")
	}
	return nil
}

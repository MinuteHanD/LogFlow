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

	"github.com/IBM/sarama"
)

const (
	InputTopic    = "raw_logs"
	OutputTopic   = "parsed_logs"
	ConsumerGroup = "parser-group"
)

type StructuredLog struct {
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
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
	handler := &logHandler{
		producer: producer,
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
	log.Println("Parser consumer is up and running!")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Termination signal received. Shutting down gracefully...")
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
	producer sarama.SyncProducer
	ready    chan bool
}

func (h *logHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *logHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *logHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Received message on topic %s: %s", message.Topic, string(message.Value))

		var logData StructuredLog
		if err := json.Unmarshal(message.Value, &logData); err != nil {
			log.Printf("Invalid JSON, skipping: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		cleanLog, err := json.Marshal(logData)
		if err != nil {
			log.Printf("Failed to re-marshal log: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		producerMsg := &sarama.ProducerMessage{
			Topic: OutputTopic,
			Value: sarama.ByteEncoder(cleanLog),
		}

		_, _, err = h.producer.SendMessage(producerMsg)
		if err != nil {
			log.Printf("Failed to send parsed log to Kafka: %v", err)
		} else {
			log.Printf("Successfully forwarded parsed log to topic %s", OutputTopic)
		}

		session.MarkMessage(message, "")
	}
	return nil
}

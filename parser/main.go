package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

const (
	KafkaBrokers  = "localhost:9092"
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
	log.Println("Starting consumer group...")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaBrokers}, ConsumerGroup, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	producer, err := newProducer()
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

	<-handler.ready // Wait until the consumer has connected and is ready.
	log.Println("Sarama consumer up and running! Waiting for messages...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	log.Println("terminating: via signal")
	cancel()
	wg.Wait()
}

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0
	return sarama.NewSyncProducer([]string{KafkaBrokers}, config)
}

type logHandler struct {
	producer sarama.SyncProducer
	ready    chan bool
}

func (handler *logHandler) Setup(sarama.ConsumerGroupSession) error {
	close(handler.ready)
	return nil
}

func (handler *logHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (handler *logHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Received message from topic %s: %s", message.Topic, string(message.Value))

		var logData StructuredLog

		if err := json.Unmarshal(message.Value, &logData); err != nil {
			log.Printf("Failed to unmarshal log: %v. Skipping.", err)
			session.MarkMessage(message, "")
			continue
		}

		cleanLog, err := json.Marshal(logData)
		if err != nil {
			log.Printf("Failed to marshal clean log: %v. Skipping.", err)
			session.MarkMessage(message, "")
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: OutputTopic,
			Value: sarama.ByteEncoder(cleanLog),
		}
		_, _, err = handler.producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to send parsed message: %v", err)
		} else {
			log.Printf("Successfully parsed and sent message to topic %s", OutputTopic)
		}

		session.MarkMessage(message, "")
	}
	return nil
}

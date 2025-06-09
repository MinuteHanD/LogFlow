package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	KafkaTopic = "raw_logs"
)

var KafkaBrokers = os.Getenv("KAFKA_BROKERS")

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0

	KafkaBrokers := os.Getenv("KAFKA_BROKERS")
	brokers := strings.Split(KafkaBrokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer producer.Close()

	router := gin.Default()
	router.POST("/log", func(c *gin.Context) {
		body, err := c.GetRawData()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
		if len(body) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "log message cannot be empty"})
			return
		}

		msg := &sarama.ProducerMessage{
			Topic: KafkaTopic,
			Value: sarama.ByteEncoder(body),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to send message to kafka",
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message":   "log received successfully",
			"partition": partition,
			"offset":    offset,
		})
	})

	log.Println("Ingestor service starting on port 8081")
	if err := router.Run(":8081"); err != nil {
		log.Fatalf("Failed to run Gin server: %v", err)
	}
}

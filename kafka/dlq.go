package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"log/slog"
)

func SendToDLQ(logger *slog.Logger, producer sarama.SyncProducer, dlqTopic string, originalMessage *sarama.ConsumerMessage, processingError error) {
	dlqMessage := &sarama.ProducerMessage{
		Topic: dlqTopic,
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
		logger.Error("CRITICAL: Failed to send message to DLQ", "topic", dlqTopic, "error", err)
	} else {
		logger.Info("Message sent to DLQ", "topic", dlqTopic, "reason", processingError.Error())
	}
}

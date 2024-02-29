package kafka

import (
	"github.com/IBM/sarama"
	"log"
)

type Config struct {
	producer    sarama.SyncProducer
	topic       string
	ServiceName *string
}

func NewConfig(brokers []string, topic string, serviceName *string) (*Config, error) {
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatalln(err)
	}

	return &Config{
		producer:    producer,
		topic:       topic,
		ServiceName: serviceName,
	}, nil
}

func (eh *Config) Close() {
	if err := eh.producer.Close(); err != nil {
		log.Printf("failed to close Kafka producer: %v\n", err)
	}
}

func (eh *Config) Send(errorJSON []byte) {
	message := &sarama.ProducerMessage{
		Topic: eh.topic,
		Value: sarama.StringEncoder(errorJSON),
	}
	partition, offset, err := eh.producer.SendMessage(message)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}

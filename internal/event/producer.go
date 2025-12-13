package event

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

// PaymentEvent is what we send to Kafka
type PaymentEvent struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	Amount    int64  `json:"amount"` // pence
}

// Producer interface allows us to swap this out for testing later
type Producer interface {
	PublishPayment(paymentID, status string, amount int64) error
	Close() error
}

// SaramaProducer is the concrete implementation
type SaramaProducer struct {
	producer sarama.SyncProducer
}

// NewSaramaProducer connects to Kafka
func NewSaramaProducer(brokers []string) (*SaramaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for leader + replicas
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to start sarama producer: %w", err)
	}

	return &SaramaProducer{producer: producer}, nil
}

// PublishPayment sends the message
func (p *SaramaProducer) PublishPayment(paymentID, status string, amount int64) error {
	event := PaymentEvent{
		PaymentID: paymentID,
		Status:    status,
		Amount:    amount,
	}

	bytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: "payments",
		Key:   sarama.StringEncoder(paymentID), // Ensure ordering by ID
		Value: sarama.ByteEncoder(bytes),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("✅ Kafka Event sent: %s (Partition: %d, Offset: %d)", paymentID, partition, offset)
	return nil
}

func (p *SaramaProducer) Close() error {
	return p.producer.Close()
}

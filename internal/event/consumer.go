package event

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type PaymentConsumerHandler struct{}

func (h *PaymentConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *PaymentConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim loops over messages from Kafka
func (h *PaymentConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event PaymentEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("⚠️ Failed to unmarshal event: %v", err)
			continue
		}

		// --- BUSINESS LOGIC SIMULATION ---
		log.Printf("🕵️  FRAUD CHECK: Processing Payment %s for £%.2f", event.PaymentID, float64(event.Amount)/100.0)
		session.MarkMessage(msg, "")
	}
	return nil
}

// RunConsumer is the entry point to start the worker
func RunConsumer(ctx context.Context, brokers []string, groupID string) error {

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return err
	}

	handler := &PaymentConsumerHandler{}

	// Loop to handle rebalances
	for {
		if err := consumerGroup.Consume(ctx, []string{"payments"}, handler); err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				return nil
			}
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

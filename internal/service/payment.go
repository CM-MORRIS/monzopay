package service

import (
	"context"
	"fmt"
	"time"

	"github.com/CM-MORRIS/monzopay/internal/repository"
	"github.com/gocql/gocql"
)

type PaymentService struct {
	repo repository.PaymentRepository
}

func NewPaymentService(repo repository.PaymentRepository) *PaymentService {
	return &PaymentService{repo: repo}
}

// ProcessPayment handles the business logic
func (s *PaymentService) ProcessPayment(ctx context.Context, idempotencyKey, from, to string, amount int64) (string, string, error) {
	if idempotencyKey == "" {
		idempotencyKey = gocql.TimeUUID().String()
	}

	// 1. Check Idempotency
	existing, err := s.repo.GetByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return "", "", fmt.Errorf("repo error: %w", err)
	}
	if existing != nil {
		return existing.ID, existing.Status, nil
	}

	// 2. Create New Payment
	paymentID := "pay_" + time.Now().Format("20060102_150405") + "_" + gocql.TimeUUID().String()[:8]
	newPayment := &repository.Payment{
		ID:             paymentID,
		IdempotencyKey: idempotencyKey,
		FromAccount:    from,
		ToAccount:      to,
		Amount:         amount,
		Status:         "ACCEPTED",
		CreatedAt:      time.Now(),
	}

	if err := s.repo.Create(ctx, newPayment); err != nil {
		return "", "", fmt.Errorf("failed to create payment: %w", err)
	}

	return paymentID, "ACCEPTED", nil
}

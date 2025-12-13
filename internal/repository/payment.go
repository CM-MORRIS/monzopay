package repository

import (
	"context"
	"time"

	"github.com/gocql/gocql"
)

// Payment represents our data model
type Payment struct {
	ID             string
	IdempotencyKey string
	FromAccount    string
	ToAccount      string
	Amount         int64
	Status         string
	CreatedAt      time.Time
}

type PaymentRepository interface {
	GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error)
	Create(ctx context.Context, p *Payment) error
}

type CassandraRepository struct {
	Session *gocql.Session
}

func NewCassandraRepository(session *gocql.Session) *CassandraRepository {
	return &CassandraRepository{Session: session}
}

func (r *CassandraRepository) GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error) {
	var p Payment
	err := r.Session.Query(`SELECT payment_id, status FROM monzopay.payments WHERE idempotency_key = ?`, key).
		Scan(&p.ID, &p.Status)

	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *CassandraRepository) Create(ctx context.Context, p *Payment) error {
	return r.Session.Query(`INSERT INTO monzopay.payments 
        (idempotency_key, payment_id, from_account, to_account, amount, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.IdempotencyKey, p.ID, p.FromAccount, p.ToAccount, p.Amount, p.Status, p.CreatedAt).Exec()
}

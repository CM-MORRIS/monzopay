package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/CM-MORRIS/monzopay/generated"
	"github.com/gocql/gocql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	// TRACING ONLY - no Kafka!
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

var cassandra *gocql.Session
var tracer trace.Tracer // Global tracer

func init() {
	// Step 1: Connect WITHOUT keyspace
	for i := 0; i < 30; i++ {
		cluster := gocql.NewCluster("cassandra")
		cluster.Consistency = gocql.Quorum
		cluster.Timeout = time.Second * 2

		session, err := cluster.CreateSession()
		if err == nil {
			// Step 2: Create keyspace/table
			session.Query(`CREATE KEYSPACE IF NOT EXISTS monzopay WITH replication = {'class':'SimpleStrategy','replication_factor':1}`).Exec()
			session.Query(`CREATE TABLE IF NOT EXISTS monzopay.payments (
                idempotency_key text PRIMARY KEY,
                payment_id text,
                from_account text,
                to_account text,
                amount double,
                status text,
                created_at timestamp
            )`).Exec()
			session.Close()

			// Step 3: Reconnect WITH keyspace
			cluster.Keyspace = "monzopay"
			var err2 error
			cassandra, err2 = cluster.CreateSession()
			if err2 == nil {
				log.Println("✅ Cassandra ready!")
				return
			}
		}

		log.Printf("Cassandra retry %d/30: %v", i+1, err)
		time.Sleep(time.Second)
	}

	log.Fatal("Cassandra failed after 30 retries")
}

// NEW: Initialize OpenTelemetry (connects to your Tempo container)
func initTracing() {
	exporter, err := otlptracegrpc.New(context.Background(),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("tempo:4317"),
	)
	if err != nil {
		log.Printf("Tracing warning (OK for now): %v", err)
		return
	}

	resource, _ := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceNameKey.String("monzopay")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
	)
	otel.SetTracerProvider(tp)
	tracer = otel.Tracer("monzopay")
	log.Println("OpenTelemetry → Tempo ready!")
}

type server struct {
	pb.UnimplementedPaymentServiceServer
}

func (s *server) Healthz(context.Context, *emptypb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "OK"}, nil
}

func (s *server) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	ctx, span := tracer.Start(ctx, "ProcessPayment", trace.WithAttributes(
		attribute.String("payment.from_account", req.FromAccount),
		attribute.String("payment.to_account", req.ToAccount),
		attribute.Float64("payment.amount", req.Amount),
		attribute.String("payment.idempotency_key", req.IdempotencyKey),
	))
	defer span.End()

	key := req.IdempotencyKey
	if key == "" {
		key = gocql.TimeUUID().String()
	}

	// Check idempotency key
	_, dbSpan1 := tracer.Start(ctx, "Cassandra.IdempotencyCheck")
	var existingID, paymentStatus string // "paymentStatus" not "status"
	err := cassandra.Query(`SELECT payment_id, status FROM monzopay.payments WHERE idempotency_key = ?`, key).
		Scan(&existingID, &paymentStatus)
	dbSpan1.End()

	if err == nil {
		span.AddEvent("✅ Idempotency hit")
		return &pb.ProcessPaymentResponse{
			PaymentId: existingID,
			Status:    paymentStatus,
			TraceId:   span.SpanContext().TraceID().String(),
		}, nil
	}

	paymentID := "pay_" + time.Now().Format("20060102_150405") + "_" + gocql.TimeUUID().String()[:8]

	// Insert new payment
	_, dbSpan2 := tracer.Start(ctx, "Cassandra.InsertPayment")
	err = cassandra.Query(`INSERT INTO monzopay.payments 
        (idempotency_key, payment_id, from_account, to_account, amount, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'ACCEPTED', ?)`,
		key, paymentID, req.FromAccount, req.ToAccount, req.Amount, time.Now()).Exec()
	dbSpan2.End()

	if err != nil {
		span.RecordError(err)
		return nil, status.Error(codes.Internal, "Database error")
	}

	span.AddEvent(fmt.Sprintf("Payment created: %s", paymentID))
	log.Printf("New payment: %s | Trace ID: %s", paymentID, span.SpanContext().TraceID().String())

	return &pb.ProcessPaymentResponse{
		PaymentId: paymentID,
		Status:    "ACCEPTED",
		TraceId:   span.SpanContext().TraceID().String(),
	}, nil
}

func main() {
	initTracing()

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPaymentServiceServer(s, &server{})
	reflection.Register(s)

	log.Printf("Tracing on :8080 → http://localhost:3000")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/CM-MORRIS/monzopay/generated"
	"github.com/CM-MORRIS/monzopay/internal/event"
	"github.com/CM-MORRIS/monzopay/internal/repository"
	"github.com/CM-MORRIS/monzopay/internal/service"
	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

var cassandra *gocql.Session

func init() {

	for i := 0; i < 30; i++ {
		cluster := gocql.NewCluster("cassandra")
		cluster.Consistency = gocql.Quorum
		cluster.Timeout = time.Second * 2
		session, err := cluster.CreateSession()
		if err == nil {
			session.Query(`CREATE KEYSPACE IF NOT EXISTS monzopay WITH replication = {'class':'SimpleStrategy','replication_factor':1}`).Exec()
			session.Query(`CREATE TABLE IF NOT EXISTS monzopay.payments (
				idempotency_key text PRIMARY KEY, 
				payment_id text, 
				from_account text, 
				to_account text, 
				amount bigint, 
				status text, 
				created_at timestamp)`).Exec()
			session.Close()
			cluster.Keyspace = "monzopay"
			var err2 error
			cassandra, err2 = cluster.CreateSession()
			if err2 == nil {
				log.Println("Cassandra ready!")
				return
			}
		}
		time.Sleep(time.Second)
	}
	log.Fatal("Cassandra failed")
}

func initTracing() {
	exporter, _ := otlptracegrpc.New(context.Background(), otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint("tempo:4317"))
	resource, _ := resource.New(context.Background(), resource.WithAttributes(semconv.ServiceNameKey.String("monzopay")))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resource))
	otel.SetTracerProvider(tp)
}

type server struct {
	pb.UnimplementedPaymentServiceServer
	svc *service.PaymentService
}

func (s *server) Healthz(context.Context, *emptypb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "OK"}, nil
}

func (s *server) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	id, paymentStatus, err := s.svc.ProcessPayment(ctx, req.IdempotencyKey, req.FromAccount, req.ToAccount, req.Amount)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Note: We use "paymentStatus" variable to avoid shadowing the "status" package
	return &pb.ProcessPaymentResponse{
		PaymentId: id,
		Status:    paymentStatus,
	}, nil
}

func main() {
	initTracing()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	// Run gRPC Server (Core)
	g.Go(func() error {
		return runGRPCServer(ctx)
	})

	// Run HTTP Server (Gateway)
	g.Go(func() error {
		return runRESTServer(ctx)
	})

	// Start Kafka Consumer Worker
	g.Go(func() error {
		kafkaAddr := os.Getenv("KAFKA_BROKERS")
		if kafkaAddr == "" {
			kafkaAddr = "kafka:29092"
		}

		log.Println("Starting Background Worker (Consumer)...")
		return event.RunConsumer(ctx, []string{kafkaAddr}, "monzo-fraud-group")
	})

	// 4. Handle Shutdown
	g.Go(func() error {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		select {
		case <-sigChan:
			cancel()
		case <-ctx.Done():
		}
		return nil
	})

	log.Println("MonzoPay running on :8080 (gRPC) and :8081 (HTTP)")
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}

func runGRPCServer(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		return err
	}

	// Setup Kafka Producer
	kafkaAddr := os.Getenv("KAFKA_BROKERS")
	if kafkaAddr == "" {
		kafkaAddr = "kafka:29092" // Default for Docker
	}

	producer, err := event.NewSaramaProducer([]string{kafkaAddr})
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer producer.Close() // Clean up when server stops

	repo := repository.NewCassandraRepository(cassandra)
	svc := service.NewPaymentService(repo, producer)

	s := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	pb.RegisterPaymentServiceServer(s, &server{svc: svc})
	reflection.Register(s)

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	log.Println("✅ gRPC Server listening on :8080")
	return s.Serve(lis)
}

func runRESTServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// Connect internally to gRPC
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
	conn, err := grpc.NewClient("localhost:8080", opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewPaymentServiceClient(conn)

	mux.HandleFunc("POST /payment", func(w http.ResponseWriter, r *http.Request) {
		var req pb.ProcessPaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		resp, err := client.ProcessPayment(r.Context(), &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	srv := &http.Server{
		Addr:    ":8081",
		Handler: otelhttp.NewHandler(mux, "http-gateway"),
	}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

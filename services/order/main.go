package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-go-shop/pkg/events"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"uuid"

	"github.com/segmentio/kafka-go"
)

type OrderProducer interface {
	PublishOrderPlaced(ctx context.Context, event events.OrderPlacedEvent) error
	Close() error
}

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			MaxAttempts:  3,
			WriteTimeout: 5 * time.Second,
			ReadTimeout:  5 * time.Second,
		},
	}
}

func (p *KafkaProducer) PublishOrderPlaced(ctx context.Context, event events.OrderPlacedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal order placed event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.OrderID),
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}

	return nil
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

type Handler struct {
	producer OrderProducer
}

type createOrderRequest struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

func (h *Handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.ProductID == "" || req.Quantity <= 0 {
		http.Error(w, "invalid product ID or quantity", http.StatusBadRequest)
		return
	}

	event := events.OrderPlacedEvent{
		OrderID:   uuid.New().String(),
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
	}

	// Send to Kafka
	if err := h.producer.PublishOrderPlaced(r.Context(), event); err != nil {
		log.Printf("failed to publish order: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(event.OrderID))

}

func main() {
	brokers := []string{"localhost:29092", "localhost:29093", "localhost:29094"}
	topic := "order-placed"

	producer := NewKafkaProducer(brokers, topic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("ERROR: closing kafka producer: %v", err)
		}
	}()

	handler := &Handler{producer: producer}

	mux := http.NewServeMux()
	mux.HandleFunc("/orders", handler.CreateOrder)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Канал для перехвата сигналов ОС (SIGINT, SIGTERM)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Запуск HTTP-сервера в отдельной горутине
	go func() {
		log.Println("order-service started on :8080")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server listen error: %v", err)
		}
	}()

	// Блокируемся до получения сигнала
	<-shutdown
	log.Println("shutting down order-service...")

	// Graceful shutdown с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("ERROR: graceful shutdown failed: %v", err)
	}

	log.Println("order-service exited cleanly")
}

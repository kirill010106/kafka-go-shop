package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-shop/pkg/events"
)

// Топики для результатов проверки склада
const (
	TopicInventoryReserved = "inventory-reserved"
	TopicInventoryRejected = "inventory-rejected"
)

// InventoryProducer абстрагирует отправку событий склада
type InventoryProducer interface {
	PublishStatus(ctx context.Context, topic string, event events.InventoryStatusEvent) error
	Close() error
}

type KafkaInventoryProducer struct {
	writer *kafka.Writer
}

func NewKafkaInventoryProducer(brokers []string) *KafkaInventoryProducer {
	return &KafkaInventoryProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			WriteTimeout: 5 * time.Second,
			// Поле Topic намеренно оставляем пустым, чтобы передавать топик в самом Message
		},
	}
}

func (p *KafkaInventoryProducer) PublishStatus(ctx context.Context, topic string, event events.InventoryStatusEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal inventory status event: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(event.OrderID),
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write inventory kafka message: %w", err)
	}

	return nil
}

func (p *KafkaInventoryProducer) Close() error {
	return p.writer.Close()
}

// OrderProcessor содержит бизнес-логику проверки остатков
type OrderProcessor struct {
	producer InventoryProducer
}

func NewOrderProcessor(producer InventoryProducer) *OrderProcessor {
	return &OrderProcessor{producer: producer}
}

// ProcessOrder проверяет остатки и отправляет соответствующее событие
func (op *OrderProcessor) ProcessOrder(ctx context.Context, order events.OrderPlacedEvent) error {
	var (
		targetTopic string
		statusEvent events.InventoryStatusEvent
	)

	// Симуляция склада из статьи: если заказывают больше 5 единиц — отказываем
	if order.Quantity > 5 {
		targetTopic = TopicInventoryRejected
		statusEvent = events.InventoryStatusEvent{
			OrderID: order.OrderID,
			Status:  "REJECTED",
			Reason:  "insufficient stock for requested quantity",
		}
		log.Printf("[INVENTORY] Order %s REJECTED (quantity: %d)", order.OrderID, order.Quantity)
	} else {
		targetTopic = TopicInventoryReserved
		statusEvent = events.InventoryStatusEvent{
			OrderID: order.OrderID,
			Status:  "RESERVED",
		}
		log.Printf("[INVENTORY] Order %s RESERVED (quantity: %d)", order.OrderID, order.Quantity)
	}

	return op.producer.PublishStatus(ctx, targetTopic, statusEvent)
}

func main() {
	brokers := []string{"localhost:29092", "localhost:29093", "localhost:29094"}
	incomingTopic := "order-placed"
	groupID := "inventory-group"

	// Конфигурация консьюмера
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID, // Участие в Consumer Group
		Topic:          incomingTopic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // 0 означает явный синхронный коммит через CommitMessages
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("ERROR: closing reader: %v", err)
		}
	}()

	producer := NewKafkaInventoryProducer(brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("ERROR: closing producer: %v", err)
		}
	}()

	processor := NewOrderProcessor(producer)

	// Контекст для корректного завершения по Ctrl+C / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("inventory-service started listening on 'order-placed'...")

	for {
		// 1. Читаем сообщение с учетом отмены контекста
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("shutting down consumer loop...")
				return
			}
			log.Printf("ERROR: fetching message: %v", err)
			continue
		}

		// 2. Десериализуем входящее событие
		var orderPlaced events.OrderPlacedEvent
		if err := json.Unmarshal(msg.Value, &orderPlaced); err != nil {
			log.Printf("ERROR: unmarshaling message payload: %v", err)
			// Коммитим битое сообщение, чтобы консьюмер не застрял на нем навсегда (poison pill)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// 3. Выполняем бизнес-логику и публикуем результат
		if err := processor.ProcessOrder(ctx, orderPlaced); err != nil {
			log.Printf("ERROR: processing order %s: %v", orderPlaced.OrderID, err)
			// НЕ коммитим смещение: при следующем перезапуске попробуем обработать заново
			continue
		}

		// 4. Фиксируем оффсет только после успешной обработки
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("ERROR: committing offset: %v", err)
		}
	}
}

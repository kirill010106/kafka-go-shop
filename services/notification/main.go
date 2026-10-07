package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"kafka-go-shop/pkg/events"
)

type Notifier interface {
	SendSuccess(orderID string)
	SendFailure(orderID string, reason string)
}

type ConsoleNotifier struct{}

func (n *ConsoleNotifier) SendSuccess(orderID string) {
	log.Printf("[NOTIFICATION] ✉️  Email to customer: Order %s confirmed and reserved!", orderID)
}

func (n *ConsoleNotifier) SendFailure(orderID string, reason string) {
	log.Printf("[NOTIFICATION] ✉️  Email to customer: Order %s failed. Reason: %s", orderID, reason)
}

func main() {
	brokers := []string{"localhost:29092", "localhost:29093", "localhost:29094"}
	groupID := "notification-group"
	topics := []string{"inventory-reserved", "inventory-rejected"}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    topics, // Подписка на несколько топиков одновременно
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0, // Ручной коммит
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("ERROR: closing notification reader: %v", err)
		}
	}()

	notifier := &ConsoleNotifier{}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("notification-service started (group: %s, topics: %v)...", groupID, topics)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("stopping notification-service consumer loop...")
				return
			}
			log.Printf("ERROR: fetch message: %v", err)
			continue
		}

		var statusEvent events.InventoryStatusEvent
		if err := json.Unmarshal(msg.Value, &statusEvent); err != nil {
			log.Printf("ERROR: unmarshal payload: %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// Роутинг по типу события (исходя из топика-источника)
		switch msg.Topic {
		case "inventory-reserved":
			notifier.SendSuccess(statusEvent.OrderID)
		case "inventory-rejected":
			notifier.SendFailure(statusEvent.OrderID, statusEvent.Reason)
		default:
			log.Printf("WARN: received unknown topic %s", msg.Topic)
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("ERROR: commit offset: %v", err)
		}
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/segmentio/kafka-go"

	"kafka-go-shop/pkg/events"
)

type MetricsTracker struct {
	reservedCount uint64
	rejectedCount uint64
}

func (m *MetricsTracker) TrackReserved(orderID string) {
	total := atomic.AddUint64(&m.reservedCount, 1)
	log.Printf("[ANALYTICS] 📊 Order %s counted as RESERVED (Total reserved: %d)", orderID, total)
}

func (m *MetricsTracker) TrackRejected(orderID string) {
	total := atomic.AddUint64(&m.rejectedCount, 1)
	log.Printf("[ANALYTICS] 📊 Order %s counted as REJECTED (Total rejected: %d)", orderID, total)
}

func main() {
	brokers := []string{"localhost:29092", "localhost:29093", "localhost:29094"}
	groupID := "analytics-group" // Свой независимый GroupID
	topics := []string{"inventory-reserved", "inventory-rejected"}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    topics,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("ERROR: closing analytics reader: %v", err)
		}
	}()

	tracker := &MetricsTracker{}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("analytics-service started (group: %s, topics: %v)...", groupID, topics)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("stopping analytics-service consumer loop...")
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

		switch msg.Topic {
		case "inventory-reserved":
			tracker.TrackReserved(statusEvent.OrderID)
		case "inventory-rejected":
			tracker.TrackRejected(statusEvent.OrderID)
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("ERROR: commit offset: %v", err)
		}
	}
}

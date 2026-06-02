package kafkalayer

import (
	"context"
	"encoding/json"
	"fmt"

	"kafka-producer/internal/domain/order"

	"github.com/segmentio/kafka-go"
)

// ProducerInterface - интерфейс для работы с продюсером Kafka
type ProducerInterface interface {
	SendOrderEvent(ctx context.Context, event *order.Event) error
	Close() error
}

// Producer - реализация продюсера Kafka
type Producer struct {
	writer *kafka.Writer
}

// NewProducer - конструктор продюсера
func NewProducer(writer *kafka.Writer) *Producer {
	return &Producer{
		writer: writer,
	}
}

// SendOrderEvent - отправка доменного события заказа
func (p *Producer) SendOrderEvent(ctx context.Context, event *order.Event) error {
	// Валидация события
	if err := event.Validate(); err != nil {
		return fmt.Errorf("invalid order event: %w", err)
	}

	// Сериализуем событие в JSON
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal order event: %w", err)
	}

	// Создаём Kafka сообщение
	// Key = OrderID для гарантии, что все события одного заказа в одну партицию
	msg := kafka.Message{
		Key:   []byte(event.PartitionKey()),
		Value: value,
		Time:  event.Timestamp,
	}

	// Отправляем
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	return nil
}

// Close - закрытие продюсера
func (p *Producer) Close() error {
	return p.writer.Close()
}

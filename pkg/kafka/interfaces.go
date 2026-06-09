package kafka

import "context"

// MessageHandler - интерфейс для обработки сообщений (реализуется в бизнес-логике)
type MessageHandler interface {
	HandleMessage(ctx context.Context, msg *Message) error
	OnBatchProcessed(batchSize int)
}

// DLQSender - интерфейс для отправки в Dead Letter Queue
type DLQSender interface {
	Send(ctx context.Context, originalMsg *Message, processingErr error) error
	Close() error
	IsEnabled() bool
}

// Consumer - интерфейс консьюмера Kafka
type Consumer interface {
	Start(ctx context.Context) error
	Shutdown()
	GetStats() (processed, dlq int64)
}

// Producer - интерфейс продьюсера Kafka
type Producer interface {
	Send(ctx context.Context, msg *Message) error
	SendBatch(ctx context.Context, messages []*Message) error
	Close() error
	IsEnabled() bool
}

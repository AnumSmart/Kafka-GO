package franzgoconsumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler - интерфейс для обработки сообщений
type MessageHandler interface {
	HandleMessage(record *kgo.Record) error
	OnBatchProcessed(batchSize int)
}

// DLQSender - интерфейс для отправки в DLQ (для возможности мока в тестах)
type DLQSender interface {
	Send(ctx context.Context, record *kgo.Record, err error) error
	Close() error
	IsEnabled() bool
}

// KafkaClient - интерфейс для kgo.Client (для тестирования)
type KafkaClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitUncommittedOffsets(ctx context.Context) error
	Close()
}

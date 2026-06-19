package consumer

import (
	"context"
	"fmt"
	"kafka_consumer/internal/idempotency"

	"log/slog"

	"pkg/kafka"
	franzgoconsumer "pkg/kafka/franz-go-consumer"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SimpleConsumer - простой consumer с хранилищем сообщений
type SimpleConsumer struct {
	baseConsumer kafka.Consumer
	logger       *slog.Logger
	messageStore *MessageStore
	debugEnabled bool
}

// NewSimpleConsumer - создаёт consumer с хранилищем
func NewSimpleConsumer(
	logger *slog.Logger,
	kafkaClient *kgo.Client,
	store *MessageStore,
	idempotencyCache *idempotency.IdempotencyCache,
	dlqManager kafka.DLQSender,
	debugEnabled bool,
) (kafka.Consumer, error) {

	// Создаём обработчик с идемпотентностью
	handler := NewStoreHandler(store, idempotencyCache, logger)

	// Включаем debug для хендлера, если нужно
	if debugEnabled {
		if sh, ok := handler.(*StoreHandler); ok {
			sh.SetDebug(true)
		}
	}

	// Настраиваем опции
	consumerOpts := franzgoconsumer.DefaultOptions()
	consumerOpts.EnableDebugLog = debugEnabled

	// Создаём адаптер для consumer
	consumerAdapter := franzgoconsumer.NewKafkaClientAdapter(kafkaClient)

	// Создаём базовый consumer
	baseConsumer, err := franzgoconsumer.NewBaseConsumer(
		consumerAdapter,
		handler,
		dlqManager,
		consumerOpts,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("error creating baseConsumer: %w", err)
	}

	return &SimpleConsumer{
		baseConsumer: baseConsumer,
		logger:       logger,
		messageStore: store,
		debugEnabled: debugEnabled,
	}, nil
}

// Start - запуск consumer
func (c *SimpleConsumer) Start(ctx context.Context) error {
	return c.baseConsumer.Start(ctx)
}

// Shutdown - завершение работы
func (c *SimpleConsumer) Shutdown() {
	// Выводим все сообщения перед закрытием
	c.messageStore.PrintAll()

	// Закрываем базовый consumer
	c.baseConsumer.Shutdown()
}

// GetMessageStore - возвращает хранилище
func (c *SimpleConsumer) GetMessageStore() *MessageStore {
	return c.messageStore
}

// GetStats - возвращает статистику
func (c *SimpleConsumer) GetStats() (processed int64, dlq int64) {
	return c.baseConsumer.GetStats()
}

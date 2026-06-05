package consumer

import (
	"context"
	"global_models/global_cache"
	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SimpleConsumer - простой consumer с хранилищем сообщений
type SimpleConsumer struct {
	baseConsumer *kafka.BaseConsumer
	messageStore *MessageStore
	cache        global_cache.Cache
	debugEnabled bool
}

// NewSimpleConsumer - создаёт consumer с хранилищем
func NewSimpleConsumer(client *kgo.Client, store *MessageStore, cache global_cache.Cache, debugEnabled bool) *SimpleConsumer {
	// Создаём обработчик
	handler := NewStoreHandler(store, cache)

	// Настраиваем опции (пока по умолчанию, но можно изменить)
	opts := kafka.DefaultConsumerOptions()
	opts.EnableDebugLog = debugEnabled

	// Создаём базовый consumer
	baseConsumer := kafka.NewBaseConsumer(client, handler, opts)

	return &SimpleConsumer{
		baseConsumer: baseConsumer,
		messageStore: store,
		cache:        cache,
		debugEnabled: debugEnabled,
	}
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
func (c *SimpleConsumer) GetStats() (processed int64, stored int) {
	return c.baseConsumer.GetStats(), c.messageStore.Count()
}

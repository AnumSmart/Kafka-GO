package consumer

import (
	"context"
	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SimpleConsumer - простой consumer с хранилищем сообщений
type SimpleConsumer struct {
	baseConsumer *kafka.BaseConsumer
	messageStore *MessageStore
}

// StoreHandler - обработчик, сохраняющий сообщения в хранилище
type StoreHandler struct {
	store *MessageStore
}

func NewStoreHandler(store *MessageStore) *StoreHandler {
	return &StoreHandler{
		store: store,
	}
}

// HandleMessage - реализация интерфейса MessageHandler
func (h *StoreHandler) HandleMessage(record *kgo.Record) error {
	// Сохраняем сообщение в хранилище
	h.store.AddFromKafka(
		record.Topic,
		record.Partition,
		record.Offset,
		record.Key,
		record.Value,
	)
	return nil
}

// OnBatchProcessed - вызывается после обработки батча
func (h *StoreHandler) OnBatchProcessed(batchSize int) {
	// Можно добавить дополнительную логику после обработки батча
	// Например, логирование или отправку метрик
}

// NewSimpleConsumer - создаёт consumer с хранилищем
func NewSimpleConsumer(client *kgo.Client, store *MessageStore, debugEnabled bool) *SimpleConsumer {
	// Создаём обработчик
	handler := NewStoreHandler(store)

	// Настраиваем опции (пока по умолчанию, но можно изменить)
	opts := kafka.DefaultConsumerOptions()
	opts.EnableDebugLog = debugEnabled

	// Создаём базовый consumer
	baseConsumer := kafka.NewBaseConsumer(client, handler, opts)

	return &SimpleConsumer{
		baseConsumer: baseConsumer,
		messageStore: store,
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

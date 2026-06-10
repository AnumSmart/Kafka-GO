package consumer

import (
	"context"
	"fmt"
	"global_models/global_cache"
	"pkg/kafka"
	franzgoconsumer "pkg/kafka/franz-go-consumer"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SimpleConsumer - простой consumer с хранилищем сообщений
type SimpleConsumer struct {
	baseConsumer kafka.Consumer
	baseProducer kafka.Producer
	messageStore *MessageStore
	cache        global_cache.Cache
	debugEnabled bool
}

// NewSimpleConsumer - создаёт consumer с хранилищем
// передаём которые клиенты, построенные на ymlconfig файлах
func NewSimpleConsumer(consmClient, dlqClient *kgo.Client, store *MessageStore, cache global_cache.Cache, debugEnabled bool) (kafka.Consumer, error) {
	// Создаём обработчик
	handler := NewStoreHandler(store, cache)

	// создаём мэнеджера для отсылки сообщений в DLQ
	dlqManager := franzgoconsumer.NewDLQManager(dlqClient, "orders.dlq", true)

	// Настраиваем опции (пока по умолчанию, но можно изменить)
	consumerOpts := franzgoconsumer.DefaultOptions()
	consumerOpts.EnableDebugLog = debugEnabled

	// создаём консьюмер клиент через адаптер
	consumerClient := franzgoconsumer.NewKafkaClientAdapter(consmClient)

	// Создаём базовый consumer
	baseConsumer, err := franzgoconsumer.NewBaseConsumer(consumerClient, handler, dlqManager, consumerOpts)
	if err != nil {
		return nil, fmt.Errorf("Error creation of baseConsumer:%v", err.Error())
	}

	return &SimpleConsumer{
		baseConsumer: baseConsumer,
		messageStore: store,
		cache:        cache,
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

package franzgoproducer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoProducerAdapter - адаптер для реального kgo.Client
// Реализует интерфейс ProducerClient
type kgoProducerAdapter struct {
	client *kgo.Client
}

// NewProducerAdapter - создает адаптер для kgo.Client
// Используется для тестирования и абстракции от конкретной библиотеки
func NewProducerAdapter(client *kgo.Client) ProducerClient {
	if client == nil {
		return nil
	}
	return &kgoProducerAdapter{
		client: client,
	}
}

// ProduceSync - синхронная отправка сообщений
func (a *kgoProducerAdapter) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	return a.client.ProduceSync(ctx, records...)
}

// ProduceAsync - асинхронная отправка сообщений
func (a *kgoProducerAdapter) ProduceAsync(ctx context.Context, records ...*kgo.Record) func() {
	return a.client.ProduceAsync(ctx, records...)
}

// Close - закрывает клиент
func (a *kgoProducerAdapter) Close() {
	a.client.Close()
}

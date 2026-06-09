package franzgoconsumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoClientAdapter - адаптер для реального kgo.Client
// Реализует интерфейс KafkaClient
type kgoClientAdapter struct {
	client *kgo.Client
}

// NewKafkaClientAdapter - создает адаптер для kgo.Client
// Используется для тестирования и абстракции от конкретной библиотеки
func NewKafkaClientAdapter(client *kgo.Client) KafkaClient {
	if client == nil {
		return nil
	}
	return &kgoClientAdapter{
		client: client,
	}
}

// PollFetches - получает сообщения от Kafka
func (a *kgoClientAdapter) PollFetches(ctx context.Context) kgo.Fetches {
	return a.client.PollFetches(ctx)
}

// CommitUncommittedOffsets - коммитит неподтвержденные оффсеты
func (a *kgoClientAdapter) CommitUncommittedOffsets(ctx context.Context) error {
	return a.client.CommitUncommittedOffsets(ctx)
}

// Close - закрывает клиент
// Адаптируем метод Close() (без ошибки) к интерфейсу (без ошибки)
func (a *kgoClientAdapter) Close() {
	a.client.Close()
}

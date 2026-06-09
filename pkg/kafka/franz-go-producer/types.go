package franzgoproducer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerClient - интерфейс для kgo.Client (для тестирования)
// Используется для DI и мокирования в тестах
type ProducerClient interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	ProduceAsync(ctx context.Context, records ...*kgo.Record) func()
	Close()
}

package franzgoproducer

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"pkg/kafka"
)

// Проверяем, что BaseProducer реализует интерфейс kafka.Producer
var _ kafka.Producer = (*BaseProducer)(nil)

// BaseProducer - базовый продьюсер с DI зависимостями
// Реализует интерфейс kafka.Producer
type BaseProducer struct {
	client  ProducerClient
	options *ProducerOptions

	// статистика
	messagesSent   atomic.Int64
	messagesFailed atomic.Int64
	lastSendTime   atomic.Value // time.Time

	// внутренние
	debugEnabled bool
}

// NewBaseProducer - создаёт базовый продьюсер
func NewBaseProducer(client ProducerClient, opts *ProducerOptions) (*BaseProducer, error) {
	if client == nil {
		return nil, ErrProducerNotInitialized
	}

	if opts == nil {
		opts = DefaultOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	p := &BaseProducer{
		client:       client,
		options:      opts,
		debugEnabled: opts.EnableDebugLog,
	}
	p.lastSendTime.Store(time.Time{})

	return p, nil
}

// Send - отправляет одно сообщение (реализация kafka.Producer)
func (p *BaseProducer) Send(ctx context.Context, msg *kafka.Message) error {
	if msg == nil {
		return ErrEmptyMessage
	}

	// Если топик не указан в сообщении, используем из опций
	topic := msg.Topic
	if topic == "" {
		topic = p.options.Topic
	}
	if topic == "" {
		return ErrTopicNotSpecified
	}

	// Конвертируем в kgo.Record
	record := FromKafkaMessage(msg)
	if record.Topic == "" {
		record.Topic = topic
	}

	p.logDebug("📤 Sending message: topic=%s, key=%s, size=%d bytes",
		record.Topic, string(record.Key), len(record.Value))

	// Отправка синхронно через интерфейс
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		p.messagesFailed.Add(1)
		p.logDebug("❌ Failed to send message: %v", err)
		return err
	}

	p.messagesSent.Add(1)
	p.lastSendTime.Store(time.Now())
	p.logDebug("✅ Message sent: topic=%s, partition=%d, offset=%d",
		record.Topic, results[0].Record.Partition, results[0].Record.Offset)

	return nil
}

// SendBatch - отправляет батч сообщений (реализация kafka.Producer)
func (p *BaseProducer) SendBatch(ctx context.Context, messages []*kafka.Message) error {
	if len(messages) == 0 {
		return ErrEmptyBatch
	}

	records := BatchFromKafkaMessages(messages)

	// Устанавливаем топик по умолчанию если не указан
	for i, record := range records {
		if record.Topic == "" {
			records[i].Topic = p.options.Topic
		}
	}

	p.logDebug("📦 Sending batch: %d messages", len(records))

	// Отправка батча синхронно через интерфейс
	results := p.client.ProduceSync(ctx, records...)

	// Проверяем ошибки
	var firstErr error
	successCount := 0

	for _, result := range results {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = result.Err
			}
			p.messagesFailed.Add(1)
			p.logDebug("❌ Failed to send message: topic=%s, error=%v",
				result.Record.Topic, result.Err)
		} else {
			successCount++
			p.messagesSent.Add(1)
		}
	}

	p.lastSendTime.Store(time.Now())
	p.logDebug("✅ Batch sent: %d success, %d failed", successCount, len(results)-successCount)

	return firstErr
}

// Close - закрывает продьюсер (реализация kafka.Producer)
func (p *BaseProducer) Close() error {
	log.Println("🛑 Closing producer...")
	p.client.Close()
	log.Printf("📊 Final statistics: sent=%d, failed=%d",
		p.messagesSent.Load(), p.messagesFailed.Load())
	return nil
}

// IsEnabled - возвращает статус продьюсера (реализация kafka.Producer)
func (p *BaseProducer) IsEnabled() bool {
	return p.client != nil
}

// GetStats - возвращает статистику
func (p *BaseProducer) GetStats() (sent, failed int64) {
	return p.messagesSent.Load(), p.messagesFailed.Load()
}

// GetLastSendTime - возвращает время последней отправки
func (p *BaseProducer) GetLastSendTime() time.Time {
	if val := p.lastSendTime.Load(); val != nil {
		return val.(time.Time)
	}
	return time.Time{}
}

// logDebug - условное логирование
func (p *BaseProducer) logDebug(format string, args ...interface{}) {
	if p.debugEnabled {
		log.Printf(format, args...)
	}
}

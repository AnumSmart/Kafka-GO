package kafka

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler - интерфейс для обработки сообщений
// Пользователь должен реализовать свою логику обработки
type MessageHandler interface {
	// HandleMessage - обрабатывает одно сообщение
	// Возвращает error, если обработка не удалась
	HandleMessage(record *kgo.Record) error

	// OnBatchProcessed - вызывается после обработки батча
	// Полезно для коммита, отправки метрик и т.д.
	OnBatchProcessed(batchSize int)
}

// BaseConsumer - базовый consumer с общей логикой
type BaseConsumer struct {
	client  *kgo.Client
	handler MessageHandler
	options *ConsumerOptions

	// статистика
	statsPrintInterval time.Duration
	lastStatsTime      time.Time
	messagesProcessed  int64
}

// ConsumerOptions - настройки consumer
type ConsumerOptions struct {
	StatsPrintInterval time.Duration // интервал вывода статистики
	CommitInterval     time.Duration // интервал коммита (0 = ручной)
	EnableDebugLog     bool          // включить отладочное логирование
}

// DefaultConsumerOptions - настройки по умолчанию
func DefaultConsumerOptions() *ConsumerOptions {
	return &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		CommitInterval:     0, // ручной коммит
		EnableDebugLog:     false,
	}
}

// NewBaseConsumer - создаёт базовый consumer
func NewBaseConsumer(client *kgo.Client, handler MessageHandler, opts *ConsumerOptions) *BaseConsumer {
	// если не передали опции
	if opts == nil {
		opts = DefaultConsumerOptions()
	}

	return &BaseConsumer{
		client:             client,
		handler:            handler,
		options:            opts,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}
}

// Start - запуск основного цикла потребления
func (bc *BaseConsumer) Start(ctx context.Context) error {
	log.Println("🚀 Starting base consumer with franz-go...")

	iteration := 0

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Context cancelled, stopping consumer...")
			return nil
		default:
		}

		iteration++
		if bc.options.EnableDebugLog && iteration%100 == 0 {
			log.Printf("💓 Consumer alive, iteration %d", iteration)
		}

		// 1. Poll fetches
		fetches := bc.client.PollFetches(ctx)

		// 2. Check errors
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf("❌ Fetch error: topic=%s, partition=%d, error=%v",
					err.Topic, err.Partition, err.Err)
			}
			continue
		}

		// 3. Process messages
		batchSize := bc.processFetches(fetches)

		// 4. Commit offsets (если ручной режим)
		if bc.options.CommitInterval == 0 && batchSize > 0 {
			if err := bc.client.CommitUncommittedOffsets(ctx); err != nil {
				log.Printf("⚠️ Failed to commit offsets: %v", err)
			} else if bc.options.EnableDebugLog {
				log.Printf("💾 Committed offsets for %d messages", batchSize)
			}
		}

		// 5. Callback после обработки батча
		if batchSize > 0 {
			bc.handler.OnBatchProcessed(batchSize)
		}

		// 6. Print stats
		bc.printStatsIfNeeded()
	}
}

// processFetches - обрабатывает полученные сообщения
func (bc *BaseConsumer) processFetches(fetches kgo.Fetches) int {
	iter := fetches.RecordIter()
	batchCount := 0

	for !iter.Done() {
		record := iter.Next()
		batchCount++

		// Вызываем обработчик сообщения
		if err := bc.handler.HandleMessage(record); err != nil {
			log.Printf("❌ Error handling message: topic=%s, partition=%d, offset=%d, error=%v",
				record.Topic, record.Partition, record.Offset, err)
			// Продолжаем обработку остальных сообщений
			continue
		}

		bc.messagesProcessed++

		if bc.options.EnableDebugLog {
			log.Printf("📨 Received message: topic=%s, partition=%d, offset=%d, key=%s, value=%s",
				record.Topic, record.Partition, record.Offset,
				truncateString(string(record.Key), 50),
				truncateString(string(record.Value), 100))
		}
	}

	if batchCount > 0 && bc.options.EnableDebugLog {
		log.Printf("✅ Processed batch: %d messages", batchCount)
	}

	return batchCount
}

// printStatsIfNeeded - вывод статистики
func (bc *BaseConsumer) printStatsIfNeeded() {
	if time.Since(bc.lastStatsTime) < bc.statsPrintInterval {
		return
	}

	log.Printf("📈 STATS: total messages processed: %d", bc.messagesProcessed)
	bc.lastStatsTime = time.Now()
}

// Shutdown - завершение работы
func (bc *BaseConsumer) Shutdown() {
	log.Println("🛑 Shutting down base consumer...")
	bc.client.Close()
	log.Println("✅ Kafka client closed")
	log.Printf("📊 Final statistics: total processed=%d", bc.messagesProcessed)
}

// GetStats - возвращает статистику
func (bc *BaseConsumer) GetStats() int64 {
	return bc.messagesProcessed
}

// truncateString - обрезает строку
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

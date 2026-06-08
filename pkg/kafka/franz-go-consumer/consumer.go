package franzgoconsumer

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BaseConsumer - базовый consumer с DI зависимостями
type BaseConsumer struct {
	client  KafkaClient
	handler MessageHandler
	dlq     DLQSender
	options *ConsumerOptions

	// статистика (атомарные для безопасности)
	messagesProcessed atomic.Int64
	messagesDLQ       atomic.Int64

	// внутренние
	statsPrintInterval time.Duration
	lastStatsTime      time.Time
}

// NewBaseConsumer - конструктор для DI (все зависимости передаются извне)
func NewBaseConsumer(client KafkaClient, handler MessageHandler, dlq DLQSender, opts *ConsumerOptions) *BaseConsumer {
	if opts == nil {
		opts = DefaultOptions()
	}

	return &BaseConsumer{
		client:             client,
		handler:            handler,
		dlq:                dlq,
		options:            opts,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}
}

// Start - запуск основного цикла потребления
func (bc *BaseConsumer) Start(ctx context.Context) error {
	log.Println("🚀 Starting base consumer...")

	iteration := 0

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Context cancelled, stopping consumer...")
			return nil
		default:
		}

		iteration++
		bc.logDebug(iteration%100 == 0, "💓 Consumer alive, iteration %d", iteration)

		fetches := bc.client.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			bc.handleFetchErrors(errs)
			continue
		}

		batchSize := bc.processFetches(ctx, fetches)

		bc.commitOffsets(ctx, batchSize)

		if batchSize > 0 {
			bc.handler.OnBatchProcessed(batchSize)
		}

		bc.printStatsIfNeeded()
	}
}

// processFetches - обрабатывает полученные сообщения
func (bc *BaseConsumer) processFetches(ctx context.Context, fetches kgo.Fetches) int {
	iter := fetches.RecordIter()
	batchCount := 0
	successCount := 0

	for !iter.Done() {
		record := iter.Next()
		batchCount++

		if err := bc.handler.HandleMessage(record); err != nil {
			bc.handleProcessingError(ctx, record, err)
			continue
		}

		successCount++
		bc.messagesProcessed.Add(1)
		bc.logDebug(bc.options.EnableDebugLog, "📨 Received: topic=%s, offset=%d", record.Topic, record.Offset)
	}

	bc.logDebug(bc.options.EnableDebugLog && batchCount > 0, "✅ Batch: %d total, %d success, %d dlq", batchCount, successCount, batchCount-successCount)

	return batchCount
}

// handleProcessingError - обрабатывает ошибку
func (bc *BaseConsumer) handleProcessingError(ctx context.Context, record *kgo.Record, err error) {
	log.Printf("❌ Error: topic=%s, offset=%d, error=%v", record.Topic, record.Offset, err)

	if bc.dlq != nil && bc.dlq.IsEnabled() {
		if dlqErr := bc.dlq.Send(ctx, record, err); dlqErr != nil {
			log.Printf("⚠️ Failed to send to DLQ: %v", dlqErr)
		} else {
			bc.messagesDLQ.Add(1)
		}
	}
}

// handleFetchErrors - обрабатывает ошибки получения
func (bc *BaseConsumer) handleFetchErrors(errs []kgo.FetchError) {
	for _, err := range errs {
		log.Printf("❌ Fetch error: topic=%s, partition=%d, error=%v", err.Topic, err.Partition, err.Err)
	}
}

// commitOffsets - коммитит оффсеты
func (bc *BaseConsumer) commitOffsets(ctx context.Context, batchSize int) {
	if bc.options.CommitInterval != 0 || batchSize == 0 {
		return
	}

	if err := bc.client.CommitUncommittedOffsets(ctx); err != nil {
		log.Printf("⚠️ Failed to commit offsets: %v", err)
	} else {
		bc.logDebug(bc.options.EnableDebugLog, "💾 Committed offsets for %d messages", batchSize)
	}
}

// Shutdown - завершение работы (закрывает только клиент, DLQ закрывается отдельно)
func (bc *BaseConsumer) Shutdown() {
	log.Println("🛑 Shutting down consumer...")
	bc.client.Close()
	log.Printf("📊 Final: processed=%d, dlq=%d",
		bc.messagesProcessed.Load(), bc.messagesDLQ.Load())
}

// GetStats - возвращает статистику
func (bc *BaseConsumer) GetStats() (processed, dlq int64) {
	return bc.messagesProcessed.Load(), bc.messagesDLQ.Load()
}

// printStatsIfNeeded - вывод статистики
func (bc *BaseConsumer) printStatsIfNeeded() {
	if time.Since(bc.lastStatsTime) < bc.statsPrintInterval {
		return
	}

	log.Printf("📈 STATS: processed=%d, dlq=%d",
		bc.messagesProcessed.Load(), bc.messagesDLQ.Load())
	bc.lastStatsTime = time.Now()
}

// logDebug - условное логирование
func (bc *BaseConsumer) logDebug(condition bool, format string, args ...interface{}) {
	if condition {
		log.Printf(format, args...)
	}
}

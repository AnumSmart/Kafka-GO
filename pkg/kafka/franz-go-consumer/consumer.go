package franzgoconsumer

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BaseConsumer - базовый consumer с DI зависимостями
// Реализует интерфейс kafka.Consumer
type BaseConsumer struct {
	client  KafkaClient
	handler kafka.MessageHandler // используем общий интерфейс
	dlq     kafka.DLQSender      // используем общий интерфейс
	options *ConsumerOptions

	// статистика (атомарные для безопасности)
	messagesProcessed atomic.Int64
	messagesDLQ       atomic.Int64

	// внутренние
	statsPrintInterval time.Duration
	lastStatsTime      time.Time
}

// NewBaseConsumer - конструктор для DI (все зависимости передаются извне)
func NewBaseConsumer(client KafkaClient, handler kafka.MessageHandler, dlq kafka.DLQSender, opts *ConsumerOptions) (*BaseConsumer, error) {
	if client == nil {
		return nil, ErrClientNotInitialized
	}
	if handler == nil {
		return nil, ErrHandlerNotInitialized
	}
	if opts == nil {
		opts = DefaultOptions()
	}
	opts.Validate()

	return &BaseConsumer{
		client:             client,
		handler:            handler,
		dlq:                dlq,
		options:            opts,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}, nil
}

// Start - запуск основного цикла потребления
// Реализует kafka.Consumer.Start
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

		// Конвертируем kgo.Record в общую модель kafka.Message
		msg := ToKafkaMessage(record)

		// Вызываем бизнес-обработчик с общей моделью
		if err := bc.handler.HandleMessage(ctx, msg); err != nil {
			bc.handleProcessingError(ctx, msg, err)
			continue
		}

		successCount++
		bc.messagesProcessed.Add(1)
		bc.logDebug(bc.options.EnableDebugLog, "📨 Received: topic=%s, offset=%d", record.Topic, record.Offset)
	}

	bc.logDebug(bc.options.EnableDebugLog && batchCount > 0,
		"✅ Batch: %d total, %d success, %d errors",
		batchCount, successCount, batchCount-successCount)

	return batchCount
}

// handleProcessingError - обрабатывает ошибку обработки сообщения
func (bc *BaseConsumer) handleProcessingError(ctx context.Context, msg *kafka.Message, err error) {
	log.Printf("❌ Error processing message: topic=%s, partition=%d, offset=%d, error=%v",
		msg.Topic, msg.Partition, msg.Offset, err)

	if bc.dlq != nil && bc.dlq.IsEnabled() {
		// Отправляем в DLQ используя общую модель сообщения
		if dlqErr := bc.dlq.Send(ctx, msg, err); dlqErr != nil {
			log.Printf("⚠️ Failed to send to DLQ: %v", dlqErr)
		} else {
			bc.messagesDLQ.Add(1)
		}
	} else {
		log.Printf("⚠️ DLQ disabled or not available, dropping failed message: offset=%d", msg.Offset)
	}
}

// handleFetchErrors - обрабатывает ошибки получения сообщений от Kafka
func (bc *BaseConsumer) handleFetchErrors(errs []kgo.FetchError) {
	for _, err := range errs {
		log.Printf("❌ Fetch error: topic=%s, partition=%d, error=%v",
			err.Topic, err.Partition, err.Err)
	}
}

// commitOffsets - коммитит оффсеты (только при ручном режиме)
func (bc *BaseConsumer) commitOffsets(ctx context.Context, batchSize int) {
	// Если установлен интервал авто-коммита или батч пустой - пропускаем
	if bc.options.CommitInterval != 0 || batchSize == 0 {
		return
	}

	if err := bc.client.CommitUncommittedOffsets(ctx); err != nil {
		log.Printf("⚠️ Failed to commit offsets: %v", err)
	} else {
		bc.logDebug(bc.options.EnableDebugLog, "💾 Committed offsets for %d messages", batchSize)
	}
}

// Shutdown - завершение работы
// Реализует kafka.Consumer.Shutdown
func (bc *BaseConsumer) Shutdown() {
	log.Println("🛑 Shutting down consumer...")

	// Закрываем Kafka клиент
	bc.client.Close()

	log.Printf("📊 Final statistics: processed=%d, dlq=%d",
		bc.messagesProcessed.Load(), bc.messagesDLQ.Load())
}

// GetStats - возвращает статистику
// Реализует kafka.Consumer.GetStats
func (bc *BaseConsumer) GetStats() (processed, dlq int64) {
	return bc.messagesProcessed.Load(), bc.messagesDLQ.Load()
}

// printStatsIfNeeded - вывод статистики с заданным интервалом
func (bc *BaseConsumer) printStatsIfNeeded() {
	if time.Since(bc.lastStatsTime) < bc.statsPrintInterval {
		return
	}

	log.Printf("📈 STATS: processed=%d, dlq=%d",
		bc.messagesProcessed.Load(), bc.messagesDLQ.Load())
	bc.lastStatsTime = time.Now()
}

// logDebug - условное логирование для отладки
func (bc *BaseConsumer) logDebug(condition bool, format string, args ...interface{}) {
	if condition {
		log.Printf(format, args...)
	}
}

// Проверяем, что BaseConsumer реализует интерфейс kafka.Consumer
var _ kafka.Consumer = (*BaseConsumer)(nil)

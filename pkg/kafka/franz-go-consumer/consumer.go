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

	isShuttingDown atomic.Bool  // флаг, что consumer получил сигнал на остановку
	currentBatch   atomic.Int64 // счетчик сообщений в текущей обработке

	// статистика (атомарные для безопасности)
	messagesProcessed atomic.Int64
	messagesDLQ       atomic.Int64

	// внутренние
	statsPrintInterval time.Duration
	lastStatsTime      time.Time
}

// NewBaseConsumer - конструктор для DI (все зависимости передаются извне)
// принимаем интерфейсы в параметрах, возвращаем  реализацию
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
		// 🆕 Проверяем, не инициирован ли shutdown
		if bc.isShuttingDown.Load() {
			log.Println("📋 Shutdown initiated, checking pending messages...")

			// Если текущий батч пустой, выходим
			if bc.currentBatch.Load() == 0 {
				log.Println("✅ No pending messages, exiting")
				return nil
			}
			// Если есть сообщения в обработке, даем им время завершиться
			log.Printf("⏳ Waiting for %d pending messages to complete...", bc.currentBatch.Load())
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case <-ctx.Done():
			// 🆕 Не выходим сразу, а инициируем graceful shutdown
			log.Println("🛑 Received shutdown signal, initiating graceful shutdown...")
			bc.isShuttingDown.Store(true)

			// Продолжаем цикл, но уже в режиме завершения
			// Не выходим здесь!
			continue

		default:
			// 🆕 Если не в режиме завершения, обрабатываем сообщения
			if !bc.isShuttingDown.Load() {
				iteration++
				bc.logDebug(iteration%100 == 0, "💓 Consumer alive, iteration %d", iteration)
				bc.pollAndProcessMessages(ctx)
				bc.printStatsIfNeeded()
			}
		}
	}
}

// 🆕 Выносим логику обработки в отдельный метод
func (bc *BaseConsumer) pollAndProcessMessages(ctx context.Context) {
	// Устанавливаем таймаут для poll, чтобы не блокировать надолго
	// Это позволит быстрее реагировать на shutdown
	pollCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	// получаем батч сообщений
	fetches := bc.client.PollFetches(pollCtx)

	if fetches.Empty() {
		return // Нет сообщений, просто выходим
	}

	// обрабатываем ошибки получения сообщений, если они есть
	if errs := fetches.Errors(); len(errs) > 0 {
		bc.handleFetchErrors(errs)
		return
	}

	// Обрабатываем сообщения
	bc.processFetchesWithShutdownTracking(ctx, fetches)
}

// processFetchesWithShutdownTracking - обрабатывает сообщения с подсчетом активных сообщений
func (bc *BaseConsumer) processFetchesWithShutdownTracking(ctx context.Context, fetches kgo.Fetches) int {
	iter := fetches.RecordIter()

	// 🆕 Получаем все сообщения в слайс, чтобы знать точное количество
	var records []*kgo.Record
	for !iter.Done() {
		records = append(records, iter.Next())
	}

	// проверка на отсутствие записей
	if len(records) == 0 {
		return 0
	}

	// 🆕 Устанавливаем счетчик активных сообщений
	bc.currentBatch.Store(int64(len(records)))
	defer bc.currentBatch.Store(0) // После обработки сбрасываем

	successCount := 0

	// пытаемся обработать весь батч сообщений
	for _, record := range records {
		// 🆕 Проверяем, не инициирован ли shutdown во время обработки
		if bc.isShuttingDown.Load() {
			log.Printf("⚠️ Shutdown in progress, stopping batch processing at %d/%d",
				successCount, len(records))
			// Не коммитим частично обработанный батч
			// При следующем запуске сообщения будут обработаны заново
			return successCount
		}
		// Конвертируем kgo.Record в общую модель kafka.Message
		msg := ToKafkaMessage(record)

		// Вызываем бизнес-обработчик с общей моделью
		if err := bc.handler.HandleMessage(ctx, msg); err != nil {
			bc.handleProcessingError(ctx, msg, err)
			bc.currentBatch.Add(-1)
			continue // после обработки сообщения переходим к следующему в цикле
		}

		successCount++
		bc.messagesProcessed.Add(1)
		bc.logDebug(bc.options.EnableDebugLog, "📨 Received: topic=%s, offset=%d",
			record.Topic, record.Offset)

		// 🆕 Уменьшаем счетчик активных сообщений
		bc.currentBatch.Add(-1)
	}

	// 🆕 Коммитим оффсеты только если не в режиме завершения
	if !bc.isShuttingDown.Load() {
		bc.commitOffsets(ctx, len(records))
	} else {
		log.Printf("⚠️ Skipping commit during shutdown, messages will be reprocessed")
	}

	bc.logDebug(bc.options.EnableDebugLog && len(records) > 0,
		"✅ Batch: %d total, %d success, %d errors",
		len(records), successCount, len(records)-successCount)

	// метод пока в разработке--------------------------------------------------------------------- пустышка
	if successCount > 0 {
		bc.handler.OnBatchProcessed(successCount)
	}

	return successCount
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
	log.Println("🛑 Shutting down consumer gracefully...")

	// 🆕 Инициируем graceful shutdown
	bc.isShuttingDown.Store(true)

	// 🆕 Ждем завершения обработки текущих сообщений с таймаутом
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)

	// даём задержку, с интервалом 100ms за тик, но не более deadline, чтобы обработать оставшиеся сообщения
	for bc.currentBatch.Load() > 0 {
		if time.Now().After(deadline) {
			log.Printf("⚠️ Timeout (%v) waiting for pending messages, forcing shutdown", timeout)
			break
		}
		log.Printf("⏳ Waiting for %d messages to complete processing...", bc.currentBatch.Load())
		time.Sleep(100 * time.Millisecond)
	}

	// Закрываем Kafka клиент
	bc.client.Close()

	// Закрываем DLQ producer если есть
	if bc.dlq != nil {
		if err := bc.dlq.Close(); err != nil {
			log.Printf("⚠️ Error closing DLQ: %v", err)
		}
	}

	log.Printf("📊 Final statistics: processed=%d, dlq=%d", bc.messagesProcessed.Load(), bc.messagesDLQ.Load())
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

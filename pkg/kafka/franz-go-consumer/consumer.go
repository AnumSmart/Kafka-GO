package franzgoconsumer

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync/atomic"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BaseConsumer - базовый consumer с DI зависимостями
// Поля отсортированы по убыванию размера для минимизации padding
type BaseConsumer struct {
	// === КОНФИГУРАЦИЯ И СОСТОЯНИЕ (8+ байт) ===
	statsPrintInterval time.Duration // 8 байт - интервал вывода статистики
	lastStatsTime      time.Time     // 24 байта - время последней статистики

	// === АТОМАРНЫЕ СЧЕТЧИКИ (8 байт каждый) ===
	messagesProcessed atomic.Int64 // 8 байт - всего обработано
	messagesDLQ       atomic.Int64 // 8 байт - всего в DLQ
	currentBatch      atomic.Int64 // 8 байт - текущий батч

	// === ЗАВИСИМОСТИ (интерфейсы = 16 байт) ===
	client  KafkaClient          // 16 байт - клиент Kafka
	handler kafka.MessageHandler // 16 байт - обработчик
	dlq     kafka.DLQSender      // 16 байт - отправитель DLQ

	// === УКАЗАТЕЛИ (8 байт каждый) ===
	logger  *slog.Logger     // 8 байт - логгер
	options *ConsumerOptions // 8 байт - опции

	// === ФЛАГИ (1 байт каждый) ===
	isShuttingDown atomic.Bool // 1 байт
	// padding: 7 байт (для выравнивания структуры до 8 байт)
}

// NewBaseConsumer - конструктор для DI (все зависимости передаются извне)
// принимаем интерфейсы в параметрах, возвращаем  реализацию
func NewBaseConsumer(client KafkaClient, handler kafka.MessageHandler, dlq kafka.DLQSender, opts *ConsumerOptions, logger *slog.Logger) (*BaseConsumer, error) {
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

	logger.Info("Creating BaseConsumer",
		"stats_interval", opts.StatsPrintInterval,
		"debug_enabled", opts.EnableDebugLog,
	)

	return &BaseConsumer{
		client:             client,
		handler:            handler,
		dlq:                dlq,
		options:            opts,
		logger:             logger,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}, nil
}

// Start - запуск основного цикла потребления
// Реализует kafka.Consumer.Start
func (bc *BaseConsumer) Start(ctx context.Context) error {
	bc.logger.Info("Starting BaseConsumer...",
		"stats_interval", bc.statsPrintInterval,
	)

	iteration := 0

	for {
		// Проверяем, не инициирован ли shutdown
		if bc.isShuttingDown.Load() {
			pending := bc.currentBatch.Load()
			if pending == 0 {
				bc.logger.Info("No pending messages, exiting gracefully")
				return nil
			}

			bc.logger.Debug("Waiting for pending messages to complete",
				"pending_count", pending,
			)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case <-ctx.Done():
			// 🆕 Не выходим сразу, а инициируем graceful shutdown
			bc.logger.Info("Received shutdown signal, initiating graceful shutdown...")
			bc.isShuttingDown.Store(true)

			// Продолжаем цикл, но уже в режиме завершения
			// Не выходим здесь!
			continue

		default:
			// 🆕 Если не в режиме завершения, обрабатываем сообщения
			if !bc.isShuttingDown.Load() {
				if iteration%100 == 0 {
					bc.logger.Debug("Consumer alive",
						"iteration", iteration,
						"processed", bc.messagesProcessed.Load(),
						"dlq", bc.messagesDLQ.Load(),
					)
				}

				bc.pollAndProcessMessages(ctx)
				bc.printStatsIfNeeded()
			}
		}
	}
}

// 🆕 Выносим логику обработки в отдельный метод
func (bc *BaseConsumer) pollAndProcessMessages(ctx context.Context) {
	// Проверяем контекст до вызова Poll
	select {
	case <-ctx.Done():
		return
	default:
	}

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
	// этот дефер вызовется с теми значениями, которые будут при завершении этой функции
	// или успешное завершение или gracefull shutdown
	defer func() {
		bc.currentBatch.Store(0) // После обработки сбрасываем
	}()

	bc.logger.Debug("Processing batch",
		"batch_size", len(records),
	)

	successCount := 0

	// пытаемся обработать весь батч сообщений
	for _, record := range records {
		// Проверяем, не инициирован ли shutdown во время обработки
		if bc.isShuttingDown.Load() {
			bc.logger.Warn("Shutdown in progress, stopping batch processing",
				"processed", successCount,
				"total", len(records),
				"remaining", len(records)-successCount,
			)
			return successCount
		}

		// Конвертируем kgo.Record в общую модель kafka.Message
		msg := ToKafkaMessage(record)

		// Вызываем бизнес-обработчик с общей моделью (безопасно, перехватываем панику)
		if err := bc.processMessageSafely(ctx, record, msg); err != nil {
			bc.handleProcessingError(ctx, msg, err)
			bc.currentBatch.Add(-1)
			continue // после обработки сообщения переходим к следующему в цикле
		}

		successCount++
		bc.messagesProcessed.Add(1)

		if bc.options.EnableDebugLog {
			bc.logger.Debug("Message processed",
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
				"key", string(record.Key),
			)
		}

		// 🆕 Уменьшаем счетчик активных сообщений
		bc.currentBatch.Add(-1)
	}

	// 🆕 Коммитим оффсеты только если не в режиме завершения
	if !bc.isShuttingDown.Load() {
		bc.commitOffsets(ctx, len(records))
	} else {
		bc.logger.Warn("Skipping commit during shutdown, messages will be reprocessed",
			"batch_size", len(records),
		)
	}

	if len(records) > 0 {
		bc.logger.Info("Batch processed",
			"total", len(records),
			"success", successCount,
			"errors", len(records)-successCount,
		)

		// Вызываем хук после обработки батча (пока пустышка)
		bc.handler.OnBatchProcessed(successCount)
	}

	return successCount
}

// метод для безопасного вызова хэндлера, если он запаникует на определённом сообщении, то это не положит консьюмер
func (bc *BaseConsumer) processMessageSafely(ctx context.Context, record *kgo.Record, msg *kafka.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler for record: %v", r)
			bc.logger.Error("Handler panicked",
				"panic", r,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
	}()
	return bc.handler.HandleMessage(ctx, msg)
}

// handleProcessingError - обрабатывает ошибку обработки сообщения
func (bc *BaseConsumer) handleProcessingError(ctx context.Context, msg *kafka.Message, err error) {
	bc.logger.Error("Failed to process message",
		"error", err,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"key", msg.Key,
	)

	if bc.dlq != nil && bc.dlq.IsEnabled() {
		// Отправляем в DLQ используя общую модель сообщения
		if dlqErr := bc.dlq.Send(ctx, msg, err); dlqErr != nil {
			bc.logger.Error("Failed to send to DLQ",
				"error", dlqErr,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		} else {
			bc.messagesDLQ.Add(1)
			bc.logger.Debug("Message sent to DLQ",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
	} else {
		bc.logger.Warn("DLQ disabled or not available, dropping failed message",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
	}
}

// handleFetchErrors - обрабатывает ошибки получения сообщений от Kafka
func (bc *BaseConsumer) handleFetchErrors(errs []kgo.FetchError) {
	for _, err := range errs {
		bc.logger.Error("Fetch error",
			"error", err.Err,
			"topic", err.Topic,
			"partition", err.Partition,
		)
	}
}

// commitOffsets - коммитит оффсеты (только при ручном режиме)
func (bc *BaseConsumer) commitOffsets(ctx context.Context, batchSize int) {
	// Если установлен интервал авто-коммита или батч пустой - пропускаем
	if bc.options.CommitInterval > 0 || batchSize == 0 {
		return
	}

	if err := bc.client.CommitUncommittedOffsets(ctx); err != nil {
		bc.logger.Error("Failed to commit offsets",
			"error", err,
			"batch_size", batchSize,
		)
	} else {
		bc.logger.Debug("Offsets committed",
			"batch_size", batchSize,
		)
	}
}

// Shutdown - завершение работы
// Реализует kafka.Consumer.Shutdown
func (bc *BaseConsumer) Shutdown() {
	bc.logger.Info("Shutting down consumer gracefully...")

	// 🆕 Инициируем graceful shutdown
	bc.isShuttingDown.Store(true)

	// 🆕 Ждем завершения обработки текущих сообщений с таймаутом
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)

	// даём задержку, с интервалом 100ms за тик, но не более deadline, чтобы обработать оставшиеся сообщения
	for bc.currentBatch.Load() > 0 {
		if time.Now().After(deadline) {
			bc.logger.Warn("Timeout waiting for pending messages, forcing shutdown",
				"timeout", timeout,
				"pending", bc.currentBatch.Load(),
			)
			break
		}
		bc.logger.Debug("Waiting for pending messages",
			"pending", bc.currentBatch.Load(),
		)
		time.Sleep(100 * time.Millisecond)
	}

	// Закрываем Kafka клиент
	bc.client.Close()

	// Закрываем DLQ producer если есть
	if bc.dlq != nil {
		if err := bc.dlq.Close(); err != nil {
			bc.logger.Error("Error closing DLQ",
				"error", err,
			)
		}
	}

	bc.logger.Info("Consumer shutdown completed",
		"processed", bc.messagesProcessed.Load(),
		"dlq", bc.messagesDLQ.Load(),
	)
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

	bc.logger.Info("Consumer statistics",
		"processed", bc.messagesProcessed.Load(),
		"dlq", bc.messagesDLQ.Load(),
		"pending", bc.currentBatch.Load(),
	)
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

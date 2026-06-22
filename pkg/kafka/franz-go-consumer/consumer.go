package franzgoconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"pkg/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BaseConsumer - базовый consumer с DI зависимостями
// Поля отсортированы по убыванию размера для минимизации padding
type BaseConsumer struct {
	// ✅ sync.Pool для переиспользования слайсов записей
	// Это позволяет избежать постоянных аллокаций памяти при обработке батчей
	// В высоконагруженных системах это дает значительный прирост производительности
	// и снижает нагрузку на GC (Garbage Collector)
	recordPool sync.Pool
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
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer options: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Creating BaseConsumer",
		"stats_interval", opts.StatsPrintInterval,
		"debug_enabled", opts.EnableDebugLog,
	)

	consumer := &BaseConsumer{
		client:             client,
		handler:            handler,
		dlq:                dlq,
		options:            opts,
		logger:             logger,
		statsPrintInterval: opts.StatsPrintInterval,
		lastStatsTime:      time.Now(),
	}

	// ✅ Инициализируем pool с функцией создания нового слайса
	// Эта функция вызывается только когда pool пуст и нужен новый объект
	// Важно: New вызывается редко, в основном объекты переиспользуются
	consumer.recordPool = sync.Pool{
		// New создает новый слайс с начальной емкостью
		// Емкость выбираем равной MaxBatchSize, чтобы сразу выделить достаточно памяти
		// Это предотвращает дополнительные переаллокации при росте слайса
		New: func() interface{} {
			// Создаем слайс с capacity = MaxBatchSize (по умолчанию 1000)
			// Если MaxBatchSize = 0, используем 1000 как значение по умолчанию
			capacity := consumer.options.MaxBatchSize
			if capacity == 0 {
				capacity = 1000
			}
			// Возвращаем указатель на слайс, чтобы избежать копирования при передаче
			s := make([]*kgo.Record, 0, capacity)
			return &s
		},
	}

	return consumer, nil
}

// Start - запуск основного цикла потребления
// Реализует kafka.Consumer.Start
func (bc *BaseConsumer) Start(ctx context.Context) error {
	bc.logger.Info("Starting BaseConsumer...",
		"stats_interval", bc.statsPrintInterval,
	)

	// ✅ Основной цикл с единой точкой проверки
	for {
		// ✅ Проверяем shutdown в начале каждой итерации
		if bc.isShuttingDown.Load() {
			return bc.waitForCompletion()
		}

		// ✅ Единый select для всех каналов
		select {
		case <-ctx.Done():
			bc.logger.Info("Received shutdown signal, initiating graceful shutdown...")
			bc.isShuttingDown.Store(true)
			return bc.waitForCompletion()

		default:
			// Обрабатываем сообщения
			bc.pollAndProcessMessages(ctx)
			// ✅ Проверяем после обработки
			if bc.isShuttingDown.Load() {
				return bc.waitForCompletion()
			}
			bc.printStatsIfNeeded()
		}
	}
}

// waitForCompletion - ожидает завершения обработки сообщений
func (bc *BaseConsumer) waitForCompletion() error {
	bc.logger.Info("Waiting for pending messages to complete...")

	timeout := bc.options.ShutdownTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		pending := bc.currentBatch.Load()
		if pending == 0 {
			bc.logger.Info("All pending messages processed")
			return nil
		}

		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				bc.logger.Warn("Timeout waiting for pending messages, forcing shutdown",
					"timeout", timeout,
					"pending", pending,
				)
				return nil
			}
			bc.logger.Debug("Waiting for pending messages",
				"pending", pending,
			)
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

// processFetchesWithShutdownTracking - обрабатывает сообщения с использованием sync.Pool
// Возвращает количество успешно обработанных сообщений
func (bc *BaseConsumer) processFetchesWithShutdownTracking(ctx context.Context, fetches kgo.Fetches) int {
	// Получаем итератор по записям из fetches
	iter := fetches.RecordIter()

	// ✅ ПОЛУЧАЕМ СЛАЙС ИЗ POOL
	// Get() возвращает interface{}, который нужно привести к нужному типу
	// Мы храним указатель на слайс (*[]*kgo.Record), чтобы избежать копирования
	recordsPtr := bc.recordPool.Get().(*[]*kgo.Record)

	// ✅ Разыменовываем указатель и очищаем слайс
	// Важно: обнуляем длину (len = 0), но сохраняем емкость (capacity)
	// Это позволяет переиспользовать уже выделенную память
	records := *recordsPtr
	records = records[:0] // Обнуляем длину, capacity сохраняется

	// ✅ ОБЯЗАТЕЛЬНО возвращаем слайс в pool после использования
	// Используем defer, чтобы гарантировать возврат даже при панике
	defer func() {
		// Сохраняем текущее состояние слайса обратно в указатель
		// Важно: мы не должны изменять слайс после этого
		*recordsPtr = records
		// Возвращаем указатель в pool для переиспользования
		bc.recordPool.Put(recordsPtr)
	}()

	// Определяем максимальный размер батча
	// Если не задан, используем значение по умолчанию
	maxBatch := bc.options.MaxBatchSize
	if maxBatch == 0 {
		maxBatch = 1000
	}

	// ✅ ЗАПОЛНЯЕМ СЛАЙС ЗАПИСЯМИ
	// Используем уже выделенную память (capacity), не создавая новые аллокации
	// Добавляем записи, пока есть данные и не достигнут лимит
	for !iter.Done() && len(records) < int(maxBatch) {
		// iter.Next() возвращает указатель на kgo.Record
		// Мы храним эти указатели в слайсе
		records = append(records, iter.Next())
	}

	// Проверяем, есть ли записи для обработки
	if len(records) == 0 {
		// Нет записей - возвращаем 0
		// Слайс все равно будет возвращен в pool через defer
		return 0
	}

	// ✅ УСТАНАВЛИВАЕМ СЧЕТЧИК АКТИВНЫХ СООБЩЕНИЙ
	// Этот счетчик используется для graceful shutdown
	// Он показывает, сколько сообщений сейчас обрабатывается
	bc.currentBatch.Store(int64(len(records)))

	// ✅ СБРАСЫВАЕМ СЧЕТЧИК ПРИ ЗАВЕРШЕНИИ
	// Используем defer для гарантированного сброса даже при панике
	// Это важно для корректного завершения приложения
	defer func() {
		bc.currentBatch.Store(0)
	}()

	// Логируем начало обработки батча
	bc.logger.Debug("Processing batch",
		"batch_size", len(records),
		"capacity", cap(records), // Показываем текущую емкость для отладки
	)

	// Счетчик успешно обработанных сообщений
	successCount := 0

	// ✅ ОБРАБАТЫВАЕМ КАЖДОЕ СООБЩЕНИЕ
	// Используем индекс для отслеживания прогресса
	for i, record := range records {
		// ✅ ПРОВЕРЯЕМ ШАТДАУН
		// Если получен сигнал завершения, останавливаем обработку
		// Это позволяет быстро реагировать на shutdown
		if bc.isShuttingDown.Load() {
			// Вычисляем количество оставшихся сообщений
			remaining := len(records) - i

			// Логируем остановку
			bc.logger.Warn("Shutdown in progress, stopping batch processing",
				"processed", successCount,
				"total", len(records),
				"remaining", remaining,
			)

			// Возвращаем количество успешно обработанных
			// Важно: счетчик currentBatch будет сброшен через defer
			return successCount
		}

		// Конвертируем kgo.Record в общую модель kafka.Message
		// ToKafkaMessage создает новый объект, который будет использован в DLQ
		// Это необходимо для абстракции от конкретной библиотеки
		msg := ToKafkaMessage(record)

		// ✅ ВЫЗЫВАЕМ БИЗНЕС-ОБРАБОТЧИК
		// processMessageSafely защищает от паник и возвращает ошибку
		if err := bc.processMessageSafely(ctx, msg); err != nil {
			// Ошибка обработки - отправляем в DLQ
			bc.handleProcessingError(ctx, msg, err)
			// Продолжаем обработку следующих сообщений
			// Не уменьшаем счетчик currentBatch - defer сбросит все в 0
			continue
		}

		// Увеличиваем счетчик успешно обработанных
		successCount++

		// Увеличиваем общий счетчик обработанных сообщений
		bc.messagesProcessed.Add(1)

		// Логируем успешную обработку (если включен debug)
		if bc.options.EnableDebugLog {
			bc.logger.Debug("Message processed",
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
				"key", string(record.Key),
			)
		}
	}

	// ✅ КОММИТИМ ОФФСЕТЫ
	// Коммитим только если не в режиме завершения
	// Если в режиме завершения - пропускаем коммит, сообщения будут переобработаны
	if !bc.isShuttingDown.Load() {
		// commitOffsets использует CommitUncommittedOffsets
		// Это коммитит все неподтвержденные оффсеты
		bc.commitOffsets(ctx, len(records))
	} else {
		// Логируем пропуск коммита
		bc.logger.Warn("Skipping commit during shutdown, messages will be reprocessed",
			"batch_size", len(records),
		)
	}

	// Логируем результат обработки батча
	if len(records) > 0 {
		bc.logger.Info("Batch processed",
			"total", len(records),
			"success", successCount,
			"errors", len(records)-successCount,
		)

		// Вызываем хук после обработки батча
		// OnBatchProcessed - это callback для внешнего мониторинга
		bc.handler.OnBatchProcessed(successCount)
	}

	// Возвращаем количество успешно обработанных сообщений
	return successCount
}

// метод для безопасного вызова хэндлера, если он запаникует на определённом сообщении, то это не положит консьюмер
func (bc *BaseConsumer) processMessageSafely(ctx context.Context, msg *kafka.Message) (err error) {
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

	bc.waitForCompletion()

	// Закрываем DLQ producer если есть
	if bc.dlq != nil {
		if err := bc.dlq.Close(); err != nil {
			bc.logger.Error("Error closing DLQ",
				"error", err,
			)
		}
	}

	// Закрываем Kafka клиент последним
	bc.client.Close()

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

// Проверяем, что BaseConsumer реализует интерфейс kafka.Consumer
var _ kafka.Consumer = (*BaseConsumer)(nil)

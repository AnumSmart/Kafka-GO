package franzgoconsumer

import (
	"context"
	"encoding/json"
	"log"
	"pkg/kafka"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// dlqMessage - внутреннее сообщение для очереди
type dlqMessage struct {
	originalMsg *kafka.Message
	handlingErr error
	enqueueTime time.Time
}

// dlqManager - асинхронная реализация kafka.DLQSender
type dlqManager struct {
	producer *kgo.Client
	config   *DLQManagerConfig
	topic    string // кэшированный топик DLQ

	// каналы и контроль
	msgCh  chan *dlqMessage
	stopCh chan struct{}
	wg     sync.WaitGroup

	// статистика
	queuedCount  atomic.Int64
	pendingCount atomic.Int64
	sentCount    atomic.Int64
	failedCount  atomic.Int64
	droppedCount atomic.Int64
	// статус мэнеджера
	closed atomic.Bool
}

// NewDLQManager - конструктор асинхронного DLQ менеджера
func NewDLQManager(producer *kgo.Client, originalTopic string, config *DLQManagerConfig) kafka.DLQSender {
	if config == nil {
		config = DefaultDLQManagerConfig()
	}

	dlqTopic := config.GetDLQTopic(originalTopic)

	log.Printf("🔧 Initializing ASYNC DLQ manager: enabled=%v, topic=%s, queue_size=%d, workers=%d",
		config.Enabled, dlqTopic, config.QueueSize, config.Workers)

	dm := &dlqManager{
		producer: producer,
		config:   config,
		topic:    dlqTopic,
		msgCh:    make(chan *dlqMessage, config.QueueSize),
		stopCh:   make(chan struct{}),
	}

	// Запускаем воркеров только если DLQ включен
	if config.Enabled && producer != nil && dlqTopic != "" {
		dm.startWorkers()
	}

	return dm
}

// startWorkers - запускает воркеров для асинхронной отправки
func (dm *dlqManager) startWorkers() {
	log.Printf("🚀 Starting %d DLQ workers...", dm.config.Workers)
	for i := 0; i < dm.config.Workers; i++ {
		dm.wg.Add(1)
		go dm.worker(i)
	}
}

// Send - НЕБЛОКИРУЮЩАЯ отправка в DLQ (реализация kafka.DLQSender)
func (dm *dlqManager) Send(ctx context.Context, originalMsg *kafka.Message, handlingErr error) error {
	// Сценарий 1: DLQ полностью отключен
	if !dm.config.Enabled {
		log.Printf("⚠️ DLQ is disabled, skipping message: topic=%s, offset=%d",
			originalMsg.Topic, originalMsg.Offset)
		return nil
	}

	// Сценарий 2: DLQ не готов к работе
	if !dm.IsEnabled() {
		log.Printf("❌ DLQ is not ready: topic=%s, offset=%d", originalMsg.Topic, originalMsg.Offset)
		return kafka.ErrDLQSendFailed
	}

	// Создаем сообщение для очереди
	dlqMsg := &dlqMessage{
		originalMsg: originalMsg,
		handlingErr: handlingErr,
		enqueueTime: time.Now(),
	}

	// НЕБЛОКИРУЮЩАЯ отправка в канал
	select {
	case dm.msgCh <- dlqMsg:
		dm.queuedCount.Add(1)
		dm.pendingCount.Add(1)
		log.Printf("📥 Queued message to DLQ: original_topic=%s, original_offset=%d, pending=%d",
			originalMsg.Topic, originalMsg.Offset, dm.pendingCount.Load())
		return nil

	default:
		// Буфер переполнен
		dm.droppedCount.Add(1)
		log.Printf("⚠️ DLQ buffer FULL, message DROPPED: original_topic=%s, original_offset=%d, dropped_total=%d",
			originalMsg.Topic, originalMsg.Offset, dm.droppedCount.Load())
		return kafka.ErrDLQSendFailed
	}
}

// worker - горутина для отправки сообщений в Kafka
func (dm *dlqManager) worker(workerID int) {
	defer dm.wg.Done()

	batch := make([]*dlqMessage, 0, dm.config.BatchSize)
	ticker := time.NewTicker(100 * time.Millisecond) // хардкжим интервал
	defer ticker.Stop()                              // освобождаем ресурсы

	for {
		select {
		case <-dm.stopCh:
			batch = dm.drainAndFlush(batch, workerID) // Возвращаем новый слайс
			return

		case <-ticker.C:
			batch = dm.flushBatch(batch, workerID) // Возвращаем новый слайс

		case msg, ok := <-dm.msgCh:
			if !ok {
				batch = dm.flushBatch(batch, workerID)
				return
			}

			batch = append(batch, msg)
			dm.pendingCount.Add(-1)

			if len(batch) >= dm.config.BatchSize {
				batch = dm.flushBatch(batch, workerID)
			}
		}
	}
}

// flushBatch - отправляет батч и возвращает пустой слайс
func (dm *dlqManager) flushBatch(batch []*dlqMessage, workerID int) []*dlqMessage {
	if len(batch) == 0 {
		return batch // Возвращаем тот же слайс
	}

	dm.sendBatch(batch, workerID)
	return batch[:0] // Возвращаем очищенный слайс
}

// drainAndFlush - вычитывает все оставшиеся сообщения
func (dm *dlqManager) drainAndFlush(batch []*dlqMessage, workerID int) []*dlqMessage {
	for {
		select {
		case msg, ok := <-dm.msgCh:
			if !ok {
				return dm.flushBatch(batch, workerID)
			}

			batch = append(batch, msg)
			dm.pendingCount.Add(-1)

			if len(batch) >= dm.config.BatchSize {
				batch = dm.flushBatch(batch, workerID)
			}
		default:
			return dm.flushBatch(batch, workerID)
		}
	}
}

// sendBatch - отправка батча сообщений в Kafka
func (dm *dlqManager) sendBatch(batch []*dlqMessage, workerID int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("💥 DLQ worker %d panic in sendBatch: %v", workerID, r)
			dm.failedCount.Add(int64(len(batch)))
		}
	}()

	if len(batch) == 0 {
		return
	}

	log.Printf("📤 DLQ worker %d: sending batch of %d messages to %s", workerID, len(batch), dm.topic)

	// Готовим записи, используя готовый конвертер ToDLQMessage из converter.go
	records := make([]*kgo.Record, 0, len(batch))
	for _, msg := range batch {
		// Используем существующий конвертер
		dlqMsg := ToDLQMessage(msg.originalMsg, msg.handlingErr, dm.config.ServiceName)

		value, err := json.Marshal(dlqMsg)
		if err != nil {
			log.Printf("❌ DLQ worker %d: failed to marshal message: %v", workerID, err)
			dm.failedCount.Add(1)
			continue
		}

		record := &kgo.Record{
			Topic: dm.topic,
			Key:   msg.originalMsg.Key,
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "original_topic", Value: []byte(msg.originalMsg.Topic)},
				{Key: "original_error", Value: []byte(msg.handlingErr.Error())},
				{Key: "original_offset", Value: []byte(itoa(msg.originalMsg.Offset))},
				{Key: "original_partition", Value: []byte(itoa(int64(msg.originalMsg.Partition)))},
				{Key: "dlq_worker", Value: []byte(itoa(int64(workerID)))},
			},
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		return
	}

	// Отправка с ретраями
	dm.sendWithRetries(records, workerID)

	// Обновляем статистику
	dm.sentCount.Add(int64(len(records)))
	log.Printf("✅ DLQ worker %d: sent %d messages to %s (total_sent=%d)",
		workerID, len(records), dm.topic, dm.sentCount.Load())
}

// sendWithRetries - отправка с ретраями
func (dm *dlqManager) sendWithRetries(records []*kgo.Record, workerID int) {
	var errs []error
	var lastErr error

	for retry := 0; retry <= dm.config.MaxRetries; retry++ {
		if retry > 0 {
			log.Printf("🔄 DLQ worker %d: retry %d/%d for %d messages",
				workerID, retry, dm.config.MaxRetries, len(records))
			time.Sleep(dm.config.RetryBackoff * time.Duration(retry))
		}

		// Создаем контекст с таймаутом
		ctx, cancel := context.WithTimeout(context.Background(), dm.config.SendTimeout)

		// Отправляем все сообщения
		produceResults := dm.producer.ProduceSync(ctx, records...)
		cancel()

		// Собираем ошибки
		errs = nil
		for _, result := range produceResults {
			if result.Err != nil {
				errs = append(errs, result.Err)
			}
		}

		if len(errs) == 0 {
			return // Все успешно
		}

		log.Printf("❌ DLQ worker %d: send failed (retry %d/%d): %d errors",
			workerID, retry, dm.config.MaxRetries, len(errs))

		lastErr = errs[len(errs)-1]
	}

	// Все ретраи исчерпаны
	dm.failedCount.Add(int64(len(records)))
	log.Printf("💀 DLQ worker %d: FAILED to send %d messages after %d",
		workerID, len(records), dm.config.MaxRetries)

	// Записываем в fallback лог, если включен
	if dm.config.FallbackEnabled && dm.config.FallbackLogPath != "" {
		dm.writeToFallbackFile(records, lastErr)
	}
}

// writeToFallbackFile - запись потерянных сообщений в файл
func (dm *dlqManager) writeToFallbackFile(records []*kgo.Record, err error) {
	log.Printf("⚠️ DLQ fallback: writing %d lost messages to %s", len(records), dm.config.FallbackLogPath)
	// Возможная кастомная логика
}

// Close - закрывает DLQ менеджер (graceful shutdown)
func (dm *dlqManager) Close() error {
	if !dm.config.Enabled {
		log.Printf("⚠️ DLQ Close called but DLQ is disabled")
		return nil
	}

	if dm.producer == nil {
		log.Printf("⚠️ DLQ Close called but producer is nil")
		return nil
	}

	log.Println("🛑 Closing ASYNC DLQ manager...")

	// Останавливаем приём новых сообщений
	close(dm.stopCh)

	// Закрываем канал сообщений (после stopCh, чтобы воркеры успели обработать)
	close(dm.msgCh)

	// Ждём завершения всех воркеров с таймаутом
	done := make(chan struct{})
	go func() {
		dm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("✅ DLQ workers finished gracefully")
	case <-time.After(dm.config.FlushTimeout):
		log.Printf("⚠️ DLQ shutdown timeout after %v, forcing close", dm.config.FlushTimeout)
	}

	// ❌ НЕ закрываем producer - это делает consumer, так как клиент один!

	// Финальная статистика
	log.Printf("📊 DLQ Stats: queued=%d, sent=%d, failed=%d, dropped=%d",
		dm.queuedCount.Load(),
		dm.sentCount.Load(),
		dm.failedCount.Load(),
		dm.droppedCount.Load())

	dm.closed.Store(true) // выставляем флаг, о том, что мэнеджер закрыт

	return nil
}

// IsEnabled - возвращает статус DLQ (реализация kafka.DLQSender)
func (dm *dlqManager) IsEnabled() bool {
	return dm.config.Enabled &&
		dm.producer != nil &&
		dm.topic != "" &&
		!dm.closed.Load()
}

// Stats - возвращает статистику для мониторинга
func (dm *dlqManager) Stats() (queued, sent, failed, dropped int64) {
	return dm.queuedCount.Load(),
		dm.sentCount.Load(),
		dm.failedCount.Load(),
		dm.droppedCount.Load()
}

// itoa - простой конвертер int64 в string
func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	negative := false
	if i < 0 {
		negative = true
		i = -i
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if negative {
		return "-" + string(digits)
	}
	return string(digits)
}

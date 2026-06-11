package consumer

import (
	"context"
	"encoding/json"
	"kafka_consumer/internal/idempotency"

	"log"
	"pkg/kafka"
	"time"
)

// StoreHandler – обработчик, сохраняющий сообщения в хранилище с поддержкой идемпотентности
type StoreHandler struct {
	store       *MessageStore
	cache       *idempotency.IdempotencyCache
	cachePrefix string
	cacheTTL    time.Duration
	debug       bool
}

// NewStoreHandler – конструктор обработчика
func NewStoreHandler(store *MessageStore, cache *idempotency.IdempotencyCache) kafka.MessageHandler {
	return &StoreHandler{
		store:       store,
		cache:       cache,
		cachePrefix: "event",
		cacheTTL:    24 * time.Hour,
		debug:       false,
	}
}

// SetDebug – включение/выключение отладочного логирования
func (h *StoreHandler) SetDebug(debug bool) {
	h.debug = debug
}

// HandleMessage – реализация интерфейса kafka.MessageHandler с идемпотентностью
func (h *StoreHandler) HandleMessage(ctx context.Context, msg *kafka.Message) error {
	// Парсим EventID из Value
	var envelope struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		if h.debug {
			log.Printf("⚠️ Failed to parse EventID from message: %v", err)
		}
		// fail-open: сохраняем без проверки идемпотентности
		return h.saveMessage(msg)
	}

	if envelope.EventID == "" {
		if h.debug {
			log.Printf("⚠️ Message without EventID, skipping idempotency check")
		}
		return h.saveMessage(msg)
	}

	// === Проверка идемпотентности ===
	// ✅ Используем правильный метод IsProcessed
	exists, err := h.cache.IsProcessed(ctx, envelope.EventID)
	if err != nil {
		// Ошибка Redis – fail‑open: сохраняем сообщение, но логируем
		log.Printf("⚠️ Redis check failed for EventID=%s: %v, saving message anyway", envelope.EventID, err)
		return h.saveMessage(msg)
	}

	if exists {
		if h.debug {
			log.Printf("⏭️ Duplicate event skipped: EventID=%s, Topic=%s, Partition=%d, Offset=%d",
				envelope.EventID, msg.Topic, msg.Partition, msg.Offset)
		}
		return nil // дубликат – не сохраняем
	}

	// === Сохранение сообщения ===
	if err := h.saveMessage(msg); err != nil {
		return err
	}

	// === Отметка в Redis как обработанное ===
	// ✅ Используем правильный метод MarkProcessed
	if err := h.cache.MarkProcessed(ctx, envelope.EventID); err != nil {
		log.Printf("⚠️ Failed to mark event %s as processed: %v", envelope.EventID, err)
	}

	if h.debug {
		log.Printf("✅ Event processed: EventID=%s, Topic=%s, Offset=%d",
			envelope.EventID, msg.Topic, msg.Offset)
	}
	return nil
}

// saveMessage – сохранение сообщения в хранилище
func (h *StoreHandler) saveMessage(msg *kafka.Message) error {
	h.store.AddFromKafka(
		msg.Topic,
		msg.Partition,
		msg.Offset,
		msg.Key,
		msg.Value,
	)
	return nil
}

// OnBatchProcessed – вызывается после обработки батча
func (h *StoreHandler) OnBatchProcessed(batchSize int) {
	if h.debug {
		log.Printf("📊 Batch processed: %d messages", batchSize)
	}
}

package consumer

import (
	"context"
	"encoding/json"
	"global_models/global_cache"
	"log"
	"pkg/kafka"
	"time"
)

// StoreHandler – обработчик, сохраняющий сообщения в хранилище с поддержкой идемпотентности
type StoreHandler struct {
	store       *MessageStore
	cache       global_cache.Cache
	cachePrefix string
	cacheTTL    time.Duration // теперь time.Duration для удобства
	debug       bool
}

// NewStoreHandler – конструктор обработчика, реализующего kafka.MessageHandler
func NewStoreHandler(store *MessageStore, cache global_cache.Cache) kafka.MessageHandler {
	return &StoreHandler{
		store:       store,
		cache:       cache,
		cachePrefix: "event",
		cacheTTL:    24 * time.Hour, // 24 часа
		debug:       false,
	}
}

// SetCacheTTL – установка TTL для кэша (в секундах)
func (h *StoreHandler) SetCacheTTL(ttlSeconds int) {
	if ttlSeconds > 0 {
		h.cacheTTL = time.Duration(ttlSeconds) * time.Second
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
	cacheKey := h.cachePrefix + ":" + envelope.EventID
	exists, err := h.cache.Exists(ctx, cacheKey)
	if err != nil {
		// Ошибка Redis – fail‑open: сохраняем сообщение, но логируем
		log.Printf("⚠️ Redis Exists failed for %s: %v, saving message anyway", cacheKey, err)
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

	// === Отметка в Redis ===
	if err := h.cache.Set(ctx, cacheKey, []byte("1"), h.cacheTTL); err != nil {
		log.Printf("⚠️ Failed to mark event %s as processed: %v", envelope.EventID, err)
	}
	if h.debug {
		log.Printf("✅ Event processed: EventID=%s", envelope.EventID)
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

// OnBatchProcessed – вызывается после обработки батча (можно добавить метрики)
func (h *StoreHandler) OnBatchProcessed(batchSize int) {
	// Например: log.Printf("Processed batch of %d messages", batchSize)
}

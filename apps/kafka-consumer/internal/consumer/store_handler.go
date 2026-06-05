package consumer

import (
	"context"
	"encoding/json"
	"global_models/global_cache"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// StoreHandler - обработчик, сохраняющий сообщения в хранилище с поддержкой идемпотентности
type StoreHandler struct {
	store       *MessageStore
	cache       global_cache.Cache
	cachePrefix string
	cacheTTL    int
	debug       bool
}

// NewStoreHandler - конструктор обработчика
func NewStoreHandler(store *MessageStore, cache global_cache.Cache) *StoreHandler {
	return &StoreHandler{
		store:       store,
		cache:       cache,
		cachePrefix: "event",
		cacheTTL:    86400, // 24 часа по умолчанию
		debug:       false,
	}
}

// SetCacheTTL - установка TTL для кэша
func (h *StoreHandler) SetCacheTTL(ttlSeconds int) {
	if ttlSeconds > 0 {
		h.cacheTTL = ttlSeconds
	}
}

// SetDebug - включение/выключение отладочного логирования
func (h *StoreHandler) SetDebug(debug bool) {
	h.debug = debug
}

// HandleMessage - реализация интерфейса MessageHandler с идемпотентностью
func (h *StoreHandler) HandleMessage(record *kgo.Record) error {
	// Парсим сообщение для получения EventID
	var envelope struct {
		EventID string `json:"event_id"`
	}

	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		if h.debug {
			log.Printf("⚠️ Failed to parse EventID from message: %v", err)
		}
		// Сохраняем сообщение без проверки (fail-open)
		return h.saveMessage(record)
	}

	// Если EventID пустой - пропускаем проверку идемпотентности
	if envelope.EventID == "" {
		if h.debug {
			log.Printf("⚠️ Message without EventID, skipping idempotency check")
		}
		return h.saveMessage(record)
	}

	// === ПРОВЕРКА ИДЕМПОТЕНТНОСТИ ===
	cacheKey := h.cachePrefix + ":" + envelope.EventID

	exists, err := h.cache.Exists(context.Background(), cacheKey)
	if err != nil {
		// При ошибке Redis - логируем и сохраняем сообщение (fail-open)
		log.Printf("⚠️ Redis Exists failed for %s: %v, saving message anyway", cacheKey, err)
		return h.saveMessage(record)
	}

	if exists {
		// Дубликат - пропускаем сохранение
		if h.debug {
			log.Printf("⏭️ Duplicate event skipped: EventID=%s, Topic=%s, Partition=%d, Offset=%d",
				envelope.EventID, record.Topic, record.Partition, record.Offset)
		}
		return nil
	}

	// === СОХРАНЕНИЕ СООБЩЕНИЯ ===
	if err := h.saveMessage(record); err != nil {
		return err
	}

	// === ОТМЕТКА В REDIS ===
	if err := h.cache.Set(context.Background(), cacheKey, []byte("1"), time.Duration(h.cacheTTL)); err != nil {
		// Ошибка при сохранении в Redis не фатальна для сообщения, но логируем
		log.Printf("⚠️ Failed to mark event %s as processed: %v", envelope.EventID, err)
	}

	if h.debug {
		log.Printf("✅ Event processed: EventID=%s", envelope.EventID)
	}

	return nil
}

// saveMessage - сохранение сообщения в хранилище
func (h *StoreHandler) saveMessage(record *kgo.Record) error {
	h.store.AddFromKafka(
		record.Topic,
		record.Partition,
		record.Offset,
		record.Key,
		record.Value,
	)
	return nil
}

// OnBatchProcessed - вызывается после обработки батча
func (h *StoreHandler) OnBatchProcessed(batchSize int) {
	// Можно добавить дополнительную логику после обработки батча
	// Например, логирование или отправку метрик
}

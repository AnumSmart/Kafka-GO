package consumer

import (
	"context"
	"encoding/json"
	"kafka_consumer/internal/idempotency"

	"pkg/kafka"
	"pkg/logger"
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
	// Получаем логгер (синглтон)
	log := logger.GetLogger()

	// Логируем начало обработки
	log.InfoContext(ctx, "processing message",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"key", string(msg.Key),
	)
	// Парсим EventID из Value
	var envelope struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		if h.debug {
			log.WarnContext(ctx, "failed to parse EventID from message, skipping idempotency check",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
		// fail-open: сохраняем без проверки идемпотентности
		return h.saveMessage(msg)
	}

	if envelope.EventID == "" {
		if h.debug {
			log.DebugContext(ctx, "message without EventID, skipping idempotency check",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
		return h.saveMessage(msg)
	}

	// === Проверка идемпотентности ===
	// ✅ Используем правильный метод IsProcessed
	exists, err := h.cache.IsProcessed(ctx, envelope.EventID)
	if err != nil {
		// Ошибка Redis – fail‑open: сохраняем сообщение, но логируем ошибку
		log.ErrorContext(ctx, "Redis check failed, saving message anyway (fail-open)",
			"event_id", envelope.EventID,
			"error", err,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
		return h.saveMessage(msg)
	}

	if exists {
		if h.debug {
			log.DebugContext(ctx, "duplicate event skipped (idempotency)",
				"event_id", envelope.EventID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
		return nil // дубликат – не сохраняем
	}

	// === Сохранение сообщения ===
	if err := h.saveMessage(msg); err != nil {
		log.ErrorContext(ctx, "failed to save message",
			"event_id", envelope.EventID,
			"error", err,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
		return err
	}

	// === Отметка в Redis как обработанное ===
	// ✅ Используем правильный метод MarkProcessed
	if err := h.cache.MarkProcessed(ctx, envelope.EventID); err != nil {
		log.WarnContext(ctx, "failed to mark event as processed in Redis",
			"event_id", envelope.EventID,
			"error", err,
		)
		// Не возвращаем ошибку, т.к. сообщение уже сохранено
		// Но можно добавить метрику или алерт
	}

	if h.debug {
		log.DebugContext(ctx, "event processed successfully",
			"event_id", envelope.EventID,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
	}

	log.InfoContext(ctx, "message processed successfully",
		"event_id", envelope.EventID,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

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
		log := logger.GetLogger()
		log.Info("batch processed", "batch_size", batchSize)
	}
}

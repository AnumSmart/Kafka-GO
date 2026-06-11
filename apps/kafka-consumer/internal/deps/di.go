package deps

import (
	"context"
	"fmt"
	"global_models/global_cache"

	"kafka_consumer/internal/config"
	"kafka_consumer/internal/idempotency"

	"log"
	"pkg/kafka"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Container struct {
	// ==================== КОНФИГУРАЦИЯ ====================
	config *config.ConsumerServiceConfig

	// ==================== KAFKA CONSUMER =================
	consumerKafkaClient *kgo.Client    // ✅ ОДИН клиент для всего (consumer + producer для DLQ)
	consumer            kafka.Consumer // используем интерфейс вместо конкретной реализации

	// ==================== DLQ ============================
	dlqSender kafka.DLQSender // DLQ менеджер

	// ==================== HANDLERS ========================
	messageHandler kafka.MessageHandler // бизнес-обработчик сообщений

	// ==================== ИДЕМПОТЕНТНОСТЬ =================
	redisClient      global_cache.Cache
	idempotencyCache *idempotency.IdempotencyCache

	// ==================== УПРАВЛЕНИЕ РЕСУРСАМИ ============
	closers   []func() error
	closeOnce sync.Once
	closeErr  error
}

func NewContainer(ctx context.Context, cfg *config.ConsumerServiceConfig) (*Container, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic inside DI container constructor: %v\n", r)
		}
	}()

	c := &Container{
		config:  cfg,
		closers: make([]func() error, 0),
	}

	// 1. Инициализация единого Kafka клиента
	if err := c.initKafkaClient(ctx); err != nil {
		return nil, fmt.Errorf("init kafka client: %w", err)
	}

	// 2. Инициализация Redis клиента (для идемпотентности)
	if err := c.initRedisClient(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init redis client: %w", err)
	}

	// 3. Инициализация IdempotencyCache
	if err := c.initIdempotencyCache(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init idempotency cache: %w", err)
	}

	// 4. Инициализация DLQ менеджера (использует тот же клиент)
	if err := c.initDLQManager(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init dlq manager: %w", err)
	}

	// 5. Инициализация MessageHandler (бизнес-логика)
	if err := c.initMessageHandler(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init message handler: %w", err)
	}

	// 6. Инициализация consumer (использует единый клиент)
	if err := c.initConsumer(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init consumer: %w", err)
	}

	log.Println("✅ DI container initialized successfully with single Kafka client")
	return c, nil
}

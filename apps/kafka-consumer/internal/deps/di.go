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
	consumerKafkaClient *kgo.Client
	consumer            kafka.Consumer // используем интерфейс вместо конкретной реализации

	// ==================== KAFKA DLQ ======================
	dlqClient *kgo.Client

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

	// 1. Инициализация ресурсов (Kafka client)
	if err := c.initResources(ctx); err != nil {
		return nil, fmt.Errorf("init resources: %w", err)
	}

	// 2. Инициализация IdempotencyCache
	if err := c.initIdempotencyCache(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init idempotency cache: %w", err)
	}

	// 3. Инициализация consumer
	if err := c.initConsumer(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init consumer: %w", err)
	}

	log.Println("DI container initialized successfully")
	return c, nil
}

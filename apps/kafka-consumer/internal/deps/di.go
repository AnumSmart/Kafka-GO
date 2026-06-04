package deps

import (
	"context"
	"fmt"
	"kafka-consumer/internal/config"
	"kafka-consumer/internal/consumer"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Container struct {
	// ==================== КОНФИГУРАЦИЯ ====================
	config *config.ConsumerConfig

	// ==================== KAFKA CONSUMER =================
	kafkaClient *kgo.Client
	consumer    *consumer.SimpleConsumer

	// ==================== УПРАВЛЕНИЕ РЕСУРСАМИ ============
	closers   []func() error
	closeOnce sync.Once
	closeErr  error
}

func NewContainer(ctx context.Context, cfg *config.ConsumerConfig) (*Container, error) {
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

	// 2. Инициализация consumer
	if err := c.initConsumer(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init consumer: %w", err)
	}

	log.Println("DI container initialized successfully")
	return c, nil
}

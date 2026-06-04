package deps

import (
	"context"
	"fmt"
	"kafka-consumer/internal/consumer"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	storageVolume = 100
)

func (c *Container) addCloser(closer func() error) {
	c.closers = append(c.closers, closer)
}

func (c *Container) initResources(ctx context.Context) error {
	// Получаем franz-go опции из конфига
	opts, err := c.config.GetConsumerConfig().ToKgoOptions()
	if err != nil {
		return fmt.Errorf("failed to create kgo options: %w", err)
	}

	// Создаём Kafka клиент
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}

	c.kafkaClient = client

	// Регистрируем закрытие клиента
	c.addCloser(func() error {
		c.kafkaClient.Close()
		log.Println("Kafka client connection closed")
		return nil
	})

	return nil
}

func (c *Container) initConsumer(ctx context.Context) error {
	// Создаём хранилище сообщений
	store := consumer.NewMessageStore(storageVolume)

	// Создаём consumer
	simpleConsumer := consumer.NewSimpleConsumer(
		c.kafkaClient,
		store,
		false,
	)

	if simpleConsumer == nil {
		return fmt.Errorf("failed to create simple consumer")
	}

	c.consumer = simpleConsumer

	// Добавляем в closers (если нужно закрыть какие-то ресурсы внутри consumer)
	c.addCloser(func() error {
		c.consumer.Shutdown()
		log.Println("Consumer resources cleaned up")
		return nil
	})

	log.Println("✓ Kafka consumer initialized")
	return nil
}

func (c *Container) Close() error {
	c.closeOnce.Do(func() {
		log.Println("closing container resources...")

		var errs []error

		// Закрываем ресурсы в обратном порядке
		for i := len(c.closers) - 1; i >= 0; i-- {
			if err := c.closers[i](); err != nil {
				errs = append(errs, err)
			}
		}

		if len(errs) > 0 {
			c.closeErr = fmt.Errorf("close errors: %v", errs)
		} else {
			log.Println("container resources closed successfully")
		}
	})

	return c.closeErr
}

// Геттеры
func (c *Container) GetConsumer() *consumer.SimpleConsumer {
	return c.consumer
}

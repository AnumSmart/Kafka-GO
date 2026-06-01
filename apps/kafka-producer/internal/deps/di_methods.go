package deps

import (
	"context"
	"fmt"
	"kafka-producer/internal/server"
	"kafka-producer/internal/server/handlers"
	"log"
	k "pkg/kafka"
)

// Управление ресурсами (добавляем функцию закрытия в слайс)
func (c *Container) addCloser(closer func() error) {
	c.closers = append(c.closers, closer)
}

// метод инициализации ресурсов
func (c *Container) initResources(ctx context.Context) error {
	// создаём конфиг для kafka-writer
	kafkaWriterConfig := c.config.ProdConfig.ToKafkaWriterConfig()

	// создаём экземпляр kafka-writer
	kafkaWriter, err := k.NewProducerFromConfig(kafkaWriterConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka writer: %w", err)
	}

	// если wtiter успешно создан, инициализируем его в структуре контейнера
	c.kafkaWriter = kafkaWriter

	// регистрируем функцию освобождения ресурсов
	c.addCloser(func() error {
		if err := c.kafkaWriter.Close(); err != nil {
			return fmt.Errorf("kafka writer close: %w", err)
		}
		log.Println("Kafka Writer connection closed")
		return nil
	})
	return nil
}

// внутренний метод инициализации хэндлеров
func (c *Container) initHandlers() error {
	handler := handlers.NewHandler()
	if handler == nil {
		return fmt.Errorf("failed to create kafka producer handler")
	}

	// если все успешно - инициализируем зависимость в контейнере
	c.handler = handler

	return nil
}

// метод инициализации http сервера
func (c *Container) initHTTPServer(ctx context.Context) error {
	httpConfig := c.config.ServerConfig

	// создаём HTTP сервер
	server, err := server.NewProducerServer(ctx, httpConfig, c.handler)
	if err != nil {
		return fmt.Errorf("failed to create http kafka-producer server: %w", err)
	}

	if server == nil {
		return fmt.Errorf("failed to create http kafka-producer server")
	}

	c.httpServer = server

	log.Println("✓ http server initialized")
	return nil
}

// Close закрывает ТОЛЬКО ресурсы (БД, Redis и т.д.)
// Сервер не закрывается здесь!
func (c *Container) Close() error {
	c.closeOnce.Do(func() {
		log.Println("closing container resources (DB, Redis, etc)...")

		var errs []error // объявляем переменную для ошибок

		// закрываем ресурсы в обратном порядке
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

	return c.closeErr // по умолчанию эта ошибка - nil
}

// Геттер для внешнего использования
func (c *Container) GetHTTPServer() *server.ProducerServer {
	return c.httpServer
}

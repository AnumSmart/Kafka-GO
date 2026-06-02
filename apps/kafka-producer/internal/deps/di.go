package deps

import (
	"context"
	"fmt"
	"kafka-producer/internal/config"
	kafkalayer "kafka-producer/internal/kafka_layer"
	"kafka-producer/internal/server"
	"kafka-producer/internal/server/handlers"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

// создаём структуру DI - контейнера
type Container struct {
	// ==================== КОНФИГУРАЦИЯ ====================
	config *config.ProducerConfig // конфигурация сервиса
	// ==================== ХЕНДЛЕРЫ (HTTP) =================
	handler *handlers.Handler // хэндлер для работы
	// ==================== KAFKA ===========================
	kafkaWriter   *kafka.Writer                // механизм добавления сообщений в очередь
	kafkaProducer kafkalayer.ProducerInterface // СЛОЙ KAFKA
	// ==================== Сервер (HTTP) ===================
	httpServer *server.ProducerServer // http сервер
	// ==================== УПРАВЛЕНИЕ РЕСУРСАМИ ============
	closers   []func() error // closers - список функций для закрытия ресурсов. Каждый closer вызывается только один раз
	closeOnce sync.Once      // closeOnce - гарантирует однократное закрытие ресурсов
	closeErr  error          // closeErr - ошибка, возникшая при закрытии ресурсов
}

func NewContainer(ctx context.Context, cfg *config.ProducerConfig) (*Container, error) {
	// перехватываем возможную панику
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic inside DI container constructor: %v\n", r)
		}
	}()

	// создаём начальный экземпляр контейнера, чтобы для его наполнения вызывать инициализацию зависимостей
	c := &Container{
		config:  cfg,
		closers: make([]func() error, 0),
	}

	// 1. Инициализация ресурсов
	if err := c.initResources(ctx); err != nil {
		return nil, fmt.Errorf("init resources: %w", err)
	}

	// 2. Инициализация хендлеров
	if err := c.initHandlers(); err != nil {
		c.Close()
		return nil, fmt.Errorf("init handlers: %w", err)
	}

	// 3. Инициализация http сервера
	if err := c.initHTTPServer(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("init grpc server: %w", err)
	}

	log.Println("DI container initialized successfully")
	return c, nil
}

package deps

import (
	"context"
	"fmt"
	"kafka_consumer/internal/consumer"
	"kafka_consumer/internal/idempotency"

	"log/slog"
	"time"

	"log"
	"pkg/kafka"
	franzgoconsumer "pkg/kafka/franz-go-consumer"
	"pkg/logger"
	"pkg/redis"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	storageVolume = 100
)

// addCloser - добавляет функцию закрытия ресурса
func (c *Container) addCloser(closer func() error) {
	c.closers = append(c.closers, closer)
}

// initLogger - создаёт единый логгер
func (c *Container) initLogger(ctx context.Context) error {
	// Инициализируем глобальный синглтон для этого микросервиса
	logger.InitLogger(c.config.LoggerConfig)

	// Получаем экземпляр логгера
	c.logger = logger.GetLogger()

	// Проверяем, что логгер инициализирован
	if c.logger == nil {
		return fmt.Errorf("failed to initialize logger")
	}

	c.logger.Info("logger initialized",
		"level", c.config.LoggerConfig.Level,
		"format", c.config.LoggerConfig.Format,
		"service", c.config.LoggerConfig.Service,
	)

	return nil
}

// ✅ МЕТОД: initKafkaClient - создает ЕДИНЫЙ Kafka клиент
func (c *Container) initKafkaClient(ctx context.Context) error {
	log.Println("Initializing single Kafka client (consumer + producer)...")

	// Получаем franz-go опции из единого конфига
	clientOptions, err := c.config.GetKafkaClientConfig().ToKgoOptions()
	if err != nil {
		return fmt.Errorf("failed to create kgo options: %w", err)
	}

	// Создаём ОДИН клиент для всего
	kafkaClient, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}

	c.consumerKafkaClient = kafkaClient
	c.logger.Info("✅ Kafka client initialized",
		"brokers", c.config.GetBrokers(),
		"topic", c.config.GetTopic(),
		"group", c.config.GetGroupID(),
	)

	// Регистрируем закрытие клиента
	c.addCloser(func() error {
		c.consumerKafkaClient.Close()
		c.logger.Info("Kafka client closed")
		return nil
	})

	return nil
}

// ✅ НОВЫЙ МЕТОД: initRedisClient - создает Redis клиент
func (c *Container) initRedisClient(ctx context.Context) error {
	c.logger.Info("initializing Redis client...")

	redisClient, err := redis.NewRedisCacheRepository(ctx, c.config.RedisConf)
	if err != nil {
		return fmt.Errorf("failed to create redis client: %w", err)
	}

	c.redisClient = redisClient
	c.logger.Info("✅ Redis client initialized",
		"host", c.config.RedisConf.Host,
	)

	// Регистрируем закрытие клиента
	c.addCloser(func() error {
		c.redisClient.Close()
		c.logger.Info("Redis client closed")
		return nil
	})

	return nil
}

// initIdempotencyCache - инициализация кэша для идемпотентности
func (c *Container) initIdempotencyCache(ctx context.Context) error {
	c.logger.Info("initializing idempotency cache...")

	cache, err := idempotency.NewIdempotencyCache(
		c.redisClient,
		"event",
		86400, // 24 часа по умолчанию
	)
	if err != nil {
		return fmt.Errorf("failed to create idempotency cache: %w", err)
	}

	// Health check
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("idempotency cache health check failed: %w", err)
	}

	c.idempotencyCache = cache
	c.logger.Info("✅ Idempotency cache initialized")
	return nil
}

// МЕТОД: initDLQManager - инициализация DLQ менеджера
func (c *Container) initDLQManager(ctx context.Context) error {
	kafkaCfg := c.config.GetKafkaClientConfig()

	// Проверяем, включен ли DLQ
	if !kafkaCfg.Producer.DLQ.Enabled {
		c.logger.Info("DLQ is disabled, using no-op DLQ manager")
		c.dlqSender = franzgoconsumer.NewDLQManager(nil, "", nil)
		return nil
	}

	c.logger.Info("initializing DLQ manager...")

	// Создаем DLQ менеджер с ТЕМ ЖЕ клиентом
	dlqTopic := kafkaCfg.GetDLQTopic()
	c.dlqSender = franzgoconsumer.NewDLQManager(
		c.consumerKafkaClient, // Тот же клиент!
		dlqTopic,              // Например: "orders-topic.dlq"
		c.config.DlqConfig,    // DLQ конфиг
	)

	c.logger.Info("✅ DLQ manager initialized", "topic", dlqTopic)
	return nil
}

// initMessageHandler - инициализация бизнес-обработчика сообщений
func (c *Container) initMessageHandler(ctx context.Context) error {
	c.logger.Info("initializing message handler...")

	// Создаем хранилище сообщений
	store := consumer.NewMessageStore(storageVolume)

	// Создаем обработчик с поддержкой идемпотентности
	// TODO: реализовать StoreHandler, который принимает idempotencyCache и store
	handler := consumer.NewStoreHandler(
		store,
		c.idempotencyCache,
	)

	c.messageHandler = handler
	c.logger.Info("✅ Message handler initialized", "storage_volume", storageVolume)
	return nil
}

// initConsumer - инициализация consumer (обновленный)
func (c *Container) initConsumer(ctx context.Context) error {
	c.logger.Info("initializing Kafka consumer...")

	// Создаем хранилище сообщений
	store := consumer.NewMessageStore(storageVolume)

	// ✅ Создаем SimpleConsumer с ОДНИМ клиентом
	simpleConsumer, err := consumer.NewSimpleConsumer(
		c.consumerKafkaClient, // ОДИН клиент для всего
		store,
		c.idempotencyCache,
		c.dlqSender,
		true, // debug enabled
	)
	if err != nil {
		return fmt.Errorf("failed to create simple consumer: %w", err)
	}

	c.consumer = simpleConsumer

	// Добавляем в closers
	c.addCloser(func() error {
		c.consumer.Shutdown()
		c.logger.Info("Consumer resources cleaned up")
		return nil
	})

	c.logger.Info("✅ Kafka consumer initialized")
	return nil
}

// getConsumerOptions - получает опции для consumer из конфига
func (c *Container) getConsumerOptions() *franzgoconsumer.ConsumerOptions {
	kafkaCfg := c.config.GetKafkaClientConfig()

	opts := franzgoconsumer.DefaultOptions()

	// Настройка интервала вывода статистики
	opts.StatsPrintInterval = 30 * time.Second

	// Включаем debug логирование (можно из конфига)
	opts.EnableDebugLog = true

	// CommitInterval берем из consumer конфига
	// Если DisableAutoCommit = true или CommitInterval = 0, используем ручной коммит
	if kafkaCfg.Consumer.DisableAutoCommit || kafkaCfg.Consumer.CommitInterval == 0 {
		opts.CommitInterval = 0 // ручной коммит
	} else {
		opts.CommitInterval = kafkaCfg.Consumer.CommitInterval
	}

	return opts
}

// Close - закрытие всех ресурсов
func (c *Container) Close() error {
	c.closeOnce.Do(func() {
		c.logger.Info("🛑 Closing DI container resources...")

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
			c.logger.Info("✅ Container resources closed successfully")
		}
	})

	return c.closeErr
}

// Геттеры
func (c *Container) GetConsumer() kafka.Consumer {
	return c.consumer
}

func (c *Container) GetKafkaClient() *kgo.Client {
	return c.consumerKafkaClient
}

func (c *Container) GetDLQSender() kafka.DLQSender {
	return c.dlqSender
}

func (c *Container) GetIdempotencyCache() *idempotency.IdempotencyCache {
	return c.idempotencyCache
}

func (c *Container) GetLogger() *slog.Logger {
	return c.logger
}

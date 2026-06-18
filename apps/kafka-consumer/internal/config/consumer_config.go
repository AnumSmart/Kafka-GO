package config

import (
	"fmt"
	"os"
	"pkg/configs"
	franzgoconsumer "pkg/kafka/franz-go-consumer"
	"pkg/logger"

	g "github.com/joho/godotenv"
)

const (
	envPath = "c:\\Son_Alex\\GO_projects\\KAFKA\\KafkaGo\\apps\\kafka-consumer\\.env"
)

type ConsumerServiceConfig struct {
	KafkaClientConfig *configs.KafkaClientConfig        // конфиг для клиента
	DlqConfig         *franzgoconsumer.DLQManagerConfig // конфиг для dlq
	RedisConf         *configs.RedisConfig              // конфиг для экземпляра REDIS (cache) (загружается в шаблон из pkg, данные берутся из .env файла)
	LoggerConfig      *logger.LoggerConfig
}

// загружаем конфиг-данные из .env
func LoadConfig() (*ConsumerServiceConfig, error) {
	err := g.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// проверка, что указан путь к .yml файлу
	yamlKafkaClientConfigPath := os.Getenv("KAFKA_CLIENT_CONFIG_ADDRESS_STRING")
	if yamlKafkaClientConfigPath == "" {
		// Если переменная окружения не задана - предупреждение, но можно продолжить
		// LoadFranzConsumerConfig использует дефолтный конфиг в этом случае
		fmt.Println("WARNING: KAFKA_CLIENT_CONFIG_ADDRESS_STRING is not set, using default config")
	}

	// загружаем данные из .yml файла для consumerConfig
	kafkaClientConfig, err := configs.LoadKafkaClientConfig(yamlKafkaClientConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config [kafka client config]: %s\n", err.Error())
	}

	// валидируем загруженный конфиг продьюссера
	err = kafkaClientConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error during validation of consumer config: %s\n", err.Error())
	}

	// проверка, что указан путь к .yml файлу
	dlqConfigPath := os.Getenv("DLQ_MANAGER_CONFIG_ADDRESS_STRING")
	if dlqConfigPath == "" {
		// Если переменная окружения не задана - предупреждение, но можно продолжить
		fmt.Println("WARNING: DLQ_CONFIG_ADDRESS_STRING is not set, using default config")
	}

	dlqManagerConfig, err := configs.LoadYAMLConfig[franzgoconsumer.DLQManagerConfig](dlqConfigPath, franzgoconsumer.DefaultDLQManagerConfig)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config [dlq manager config]: %s\n", err.Error())
	}

	// проверка, что указан путь к .yml файлу
	loggerConfigPath := os.Getenv("LOGGER_CONFIG_ADDRESS_STRING")
	if dlqConfigPath == "" {
		// Если переменная окружения не задана - предупреждение, но можно продолжить
		fmt.Println("WARNING: LOGGER_ADDRESS_STRING is not set, using default config")
	}

	loggerConfig, err := configs.LoadYAMLConfig[logger.LoggerConfig](loggerConfigPath, logger.DefaultLoggerConfig)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config [logger config]: %s\n", err.Error())
	}

	// загружаем данные из .env файла для redisConfig
	redisConfig, err := configs.NewRedisConfigFromEnv("REDIS_CACHE")
	if err != nil {
		return nil, fmt.Errorf("Error during loading config [redis config]: %s\n", err.Error())
	}

	return &ConsumerServiceConfig{
		KafkaClientConfig: kafkaClientConfig,
		DlqConfig:         dlqManagerConfig,
		RedisConf:         redisConfig,
		LoggerConfig:      loggerConfig,
	}, nil
}

// GetKafkaClientConfig - вспомогательный метод для получения franz-go конфига
// Упрощает доступ к вложенной структуре
func (c *ConsumerServiceConfig) GetKafkaClientConfig() *configs.KafkaClientConfig {
	return c.KafkaClientConfig
}

// GetBrokers - возвращает список брокеров
func (c *ConsumerServiceConfig) GetBrokers() []string {
	return c.KafkaClientConfig.Brokers
}

// GetTopic - возвращает название топика
func (c *ConsumerServiceConfig) GetTopic() string {
	return c.KafkaClientConfig.Topic
}

// GetGroupID - возвращает ID consumer группы
func (c *ConsumerServiceConfig) GetGroupID() string {
	return c.KafkaClientConfig.GroupID
}

package config

import (
	"fmt"
	"os"
	"pkg/configs"

	g "github.com/joho/godotenv"
)

const (
	envPath = "c:\\Son_Alex\\GO_projects\\KAFKA\\KafkaGo\\apps\\kafka-consumer\\.env"
)

type ConsumerServiceConfig struct {
	ConsumerConfig    *configs.FranzConsumerConfig
	DLQProducerConfig *configs.FranzProducerConfig
	RedisConf         *configs.RedisConfig // конфиг для экземпляра REDIS (cache) (загружается в шаблон из pkg, данные берутся из .env файла)
}

// загружаем конфиг-данные из .env
func LoadConfig() (*ConsumerServiceConfig, error) {
	err := g.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// проверка, что указан путь к .yml файлу
	yamlConsumerConfigPath := os.Getenv("CONSUMER_CONFIG_ADDRESS_STRING")
	if yamlConsumerConfigPath == "" {
		// Если переменная окружения не задана - предупреждение, но можно продолжить
		// LoadFranzConsumerConfig использует дефолтный конфиг в этом случае
		fmt.Println("WARNING: CONSUMER_CONFIG_ADDRESS_STRING is not set, using default config")
	}

	// загружаем данные из .yml файла для consumerConfig
	consumerConfig, err := configs.LoadFranzConsumerConfig(yamlConsumerConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// валидируем загруженный конфиг продьюссера
	err = consumerConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error during validation of consumer config: %s\n", err.Error())
	}

	// проверка, что указан путь к .yml файлу
	yamlDLQProducerConfigPath := os.Getenv("DLQ_PRODUCER_CONFIG_ADDRESS_STRING")
	if yamlDLQProducerConfigPath == "" {
		// Если переменная окружения не задана - предупреждение, но можно продолжить
		// LoadFranzConsumerConfig использует дефолтный конфиг в этом случае
		fmt.Println("WARNING: DLQ_PRODUCER_CONFIG_ADDRESS_STRING is not set, using default config")
	}

	// загружаем данные из .yml файла для dlqProducerConfig
	dlqProducerConfig, err := configs.LoadFranzProducerConfig(yamlDLQProducerConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// загружаем данные из .env файла для redisConfig
	redisConfig, err := configs.NewRedisConfigFromEnv("REDIS_CACHE")
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	return &ConsumerServiceConfig{
		ConsumerConfig:    consumerConfig,
		DLQProducerConfig: dlqProducerConfig,
		RedisConf:         redisConfig,
	}, nil
}

// GetConsumerConfig - вспомогательный метод для получения franz-go конфига
// Упрощает доступ к вложенной структуре
func (c *ConsumerServiceConfig) GetConsumerConfig() *configs.FranzConsumerConfig {
	return c.ConsumerConfig
}

// GetDLQProdConfig - вспомогательный метод для получения franz-go конфига для продьюссера DLQ
func (c *ConsumerServiceConfig) GetDLQProdConfig() *configs.FranzProducerConfig {
	return c.DLQProducerConfig
}

// GetBrokers - возвращает список брокеров
func (c *ConsumerServiceConfig) GetBrokers() []string {
	return c.ConsumerConfig.Brokers
}

// GetTopic - возвращает название топика
func (c *ConsumerServiceConfig) GetTopic() string {
	return c.ConsumerConfig.Topic
}

// GetGroupID - возвращает ID consumer группы
func (c *ConsumerServiceConfig) GetGroupID() string {
	return c.ConsumerConfig.GroupID
}

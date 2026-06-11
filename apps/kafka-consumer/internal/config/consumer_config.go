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
	KafkaClientConfig *configs.KafkaClientConfig
	RedisConf         *configs.RedisConfig // конфиг для экземпляра REDIS (cache) (загружается в шаблон из pkg, данные берутся из .env файла)
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
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// валидируем загруженный конфиг продьюссера
	err = kafkaClientConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error during validation of consumer config: %s\n", err.Error())
	}

	// загружаем данные из .env файла для redisConfig
	redisConfig, err := configs.NewRedisConfigFromEnv("REDIS_CACHE")
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	return &ConsumerServiceConfig{
		KafkaClientConfig: kafkaClientConfig,
		RedisConf:         redisConfig,
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

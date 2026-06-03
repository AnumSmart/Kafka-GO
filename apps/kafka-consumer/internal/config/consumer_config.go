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

type ConsumerConfig struct {
	ConsumerConfig *configs.FranzConsumerConfig
}

// загружаем конфиг-данные из .env
func LoadConfig() (*ConsumerConfig, error) {
	err := g.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// проверка, что указан путь к .yml файлу
	yamlConfigPath := os.Getenv("CONSUMER_CONFIG_ADDRESS_STRING")
	if yamlConfigPath == "" {
		// Если переменная окружения не задана - предупреждение, но можно продолжить
		// LoadFranzConsumerConfig использует дефолтный конфиг в этом случае
		fmt.Println("WARNING: CONSUMER_CONFIG_ADDRESS_STRING is not set, using default config")
	}

	// загружаем данные из .yml файла для consumerConfig
	consumerConfig, err := configs.LoadFranzConsumerConfig(yamlConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// валидируем загруженный конфиг продьюссера
	err = consumerConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error during validation of consumer config: %s\n", err.Error())
	}

	return &ConsumerConfig{
		ConsumerConfig: consumerConfig,
	}, nil
}

// GetConsumerConfig - вспомогательный метод для получения franz-go конфига
// Упрощает доступ к вложенной структуре
func (c *ConsumerConfig) GetConsumerConfig() *configs.FranzConsumerConfig {
	return c.ConsumerConfig
}

// GetBrokers - возвращает список брокеров
func (c *ConsumerConfig) GetBrokers() []string {
	return c.ConsumerConfig.Brokers
}

// GetTopic - возвращает название топика
func (c *ConsumerConfig) GetTopic() string {
	return c.ConsumerConfig.Topic
}

// GetGroupID - возвращает ID consumer группы
func (c *ConsumerConfig) GetGroupID() string {
	return c.ConsumerConfig.GroupID
}

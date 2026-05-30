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
	ServerConfig   *configs.ServerConfig
	ConsumerConfig *configs.ConsumerConfig
}

// загружаем конфиг-данные из .env
func LoadConfig() (*ConsumerConfig, error) {
	err := g.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// загружаем данные из .yml файла для serverConfig
	serverConfig, err := configs.LoadYAMLConfig[configs.ServerConfig](os.Getenv("SERVER_CONFIG_ADDRESS_STRING"), configs.UseDefaultServerConfig)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// загружаем данные из .yml файла для consumerConfig
	consumerConfig, err := configs.LoadConsumerConfig(os.Getenv("SERVER_CONFIG_ADDRESS_STRING"))
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// валидируем загруженный конфиг продьюссера
	err = consumerConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error during validation of consumer config: %s\n", err.Error())
	}

	return &ConsumerConfig{
		ServerConfig:   serverConfig,
		ConsumerConfig: consumerConfig,
	}, nil
}

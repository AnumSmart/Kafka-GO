package config

import (
	"fmt"
	"os"
	"pkg/configs"

	g "github.com/joho/godotenv"
)

const (
	envPath = "c:\\Son_Alex\\GO_projects\\KAFKA\\KafkaGo\\apps\\kafka-producer\\.env"
)

type ProducerConfig struct {
	ServerConfig *configs.ServerConfig
	ProdConfig   *configs.ProducerConfig
}

// загружаем конфиг-данные из .env
func LoadConfig() (*ProducerConfig, error) {
	err := g.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// загружаем данные из .yml файла для serverConfig
	serverConfig, err := configs.LoadYAMLConfig[configs.ServerConfig](os.Getenv("SERVER_CONFIG_ADDRESS_STRING"), configs.UseDefaultServerConfig)
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// загружаем данные из .yml файла для producerConfig
	producerConfig, err := configs.LoadProducerConfig(os.Getenv("SERVER_CONFIG_ADDRESS_STRING"))
	if err != nil {
		return nil, fmt.Errorf("Error during loading config: %s\n", err.Error())
	}

	// валидируем загруженный конфиг продьюссера
	err = producerConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error during validation of producer config: %s\n", err.Error())
	}

	return &ProducerConfig{
		ServerConfig: serverConfig,
		ProdConfig:   producerConfig,
	}, nil
}

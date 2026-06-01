package main

import (
	"context"
	"fmt"
	"kafka-producer/internal/config"
	"kafka-producer/internal/deps"
	"log"
)

// createDIContainer создает DI контейнер со всеми зависимостями
func createDIContainer(cfg *config.ProducerConfig) (*deps.Container, error) {
	log.Println("🔧 Creating DI container...")

	// Создаем контейнер (инициализирует БД, Redis, репозитории, сервисы, хендлеры)
	container, err := deps.NewContainer(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create container: %w", err)
	}

	log.Println("  ✓ Kafka Writer initialized")
	log.Println("  ✓ Handlers initialized")

	return container, nil
}

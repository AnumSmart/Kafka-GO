package main

import (
	"context"
	"kafka_consumer/internal/config"

	"log"
	"os"
)

func main() {

	log.Println("🚀 Starting Kafka Consumer Service...")
	log.Println("📦 Using franz-go library")

	// 1. Загрузка конфигурации
	log.Println("📋 Loading configuration...")

	// Загружаем .env из текущей директории
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Printf("❌ Failed to load config: %v", err)
		os.Exit(1)
	}

	log.Printf("✓ Config loaded: brokers=%v, topic=%s, group_id=%s",
		cfg.GetBrokers(), cfg.GetTopic(), cfg.GetGroupID())

	// 2. Создание DI контейнера
	log.Println("🏗️  Initializing DI container...")

	ctx := context.Background()
	container, err := NewDIContainer(ctx, cfg)
	if err != nil {
		log.Printf("❌ Failed to create DI container: %v", err)
		os.Exit(1)
	}

	// Убеждаемся, что ресурсы будут закрыты при выходе
	defer gracefulShutdownResources(container)

	log.Println("✓ DI container initialized successfully")

	// 3. Получение consumer из контейнера
	simpleConsumer := container.GetConsumer()
	if simpleConsumer == nil {
		log.Println("❌ Failed to get consumer from container")
		os.Exit(1)
	}

	log.Println("✓ Consumer ready")

	// 4. Запуск consumer (получаем канал ошибок)
	consumerErrors := startConsumer(ctx, simpleConsumer)

	// 5. Ожидание сигнала завершения
	waitForShutdown(simpleConsumer, consumerErrors)

	log.Println("👋 Kafka Consumer Service stopped")
}

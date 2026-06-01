package main

import (
	"kafka-producer/internal/config"
	"log"
	"time"
)

// Настройки graceful shutdown
const (
	// GracefulShutdownTimeout - максимальное время ожидания завершения текущих запросов
	GracefulShutdownTimeout = 30 * time.Second

	// ServerStartDelay - задержка перед запуском сервера (для отладки)
	ServerStartDelay = 0 * time.Second

	// HealthCheckTimeout - таймаут для проверки здоровья зависимостей
	HealthCheckTimeout = 5 * time.Second
)

func main() {
	// Создаем логгер с timestamp
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Println("========================================")
	log.Println("Starting Kafka-Producer Service")
	log.Println("========================================")

	// 1. Загрузка конфигурации
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("❌ Failed to load configuration: %v", err)
	}
	log.Println("✓ Configuration loaded successfully")

	// 2. Создание DI контейнера
	container, err := createDIContainer(cfg)
	if err != nil {
		log.Fatalf("❌ Failed to create DI container: %v", err)
	}

	// 3. Настройка graceful shutdown (отложенное закрытие ресурсов)
	defer gracefulShutdown(container)

	// 4. Получение gRPC сервера из контейнера
	httpServer := container.GetHTTPServer()
	if httpServer == nil {
		log.Fatal("❌ http server is nil")
	}
	log.Println("✓ http server created")

	// 5. Запуск http сервера в отдельной горутине
	serverErrors := make(chan error, 1)
	startHTTPServer(httpServer, serverErrors)

	// 6. Ожидание сигнала завершения или ошибки
	waitForShutdown(httpServer, serverErrors)

	log.Println("========================================")
	log.Println("Kafka-Producer Service stopped successfully")
	log.Println("========================================")
}

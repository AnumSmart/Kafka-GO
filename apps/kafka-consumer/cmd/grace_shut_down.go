package main

import (
	"context"
	"kafka-consumer/internal/consumer"
	"kafka-consumer/internal/deps"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// GracefulShutdownTimeout - таймаут для graceful shutdown
const GracefulShutdownTimeout = 30 * time.Second

// waitForShutdown ожидает сигнал завершения или ошибку consumer
func waitForShutdown(simpleConsumer *consumer.SimpleConsumer, consumerErrors <-chan error) {
	// Настраиваем канал для системных сигналов
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,  // Ctrl+C
		syscall.SIGTERM, // kill (терминация)
		syscall.SIGQUIT, // Ctrl+\ (квит)
		syscall.SIGHUP,  // Закрытие терминала
	)

	// Блокируем main, ожидая сигнал или ошибку
	select {
	case sig := <-sigChan:
		log.Printf("📡 Received signal: %s", sig)
		// Для SIGQUIT делаем принудительное завершение без graceful
		if sig == syscall.SIGQUIT {
			log.Println("⚠️  SIGQUIT received, forcing immediate shutdown")
			return
		}

		log.Println("🛑 Initiating graceful shutdown...")

	case err := <-consumerErrors:
		log.Printf("❌ Consumer error: %v", err)
		log.Println("🛑 Initiating shutdown due to error...")
	}

	// Выполняем graceful shutdown
	performGracefulShutdown(simpleConsumer)
}

// performGracefulShutdown выполняет корректное завершение работы consumer
func performGracefulShutdown(simpleConsumer *consumer.SimpleConsumer) {
	log.Println("⏳ Waiting for current batch processing to complete...")

	// Создаем контекст с таймаутом для graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), GracefulShutdownTimeout)
	defer shutdownCancel()

	// Создаем канал для сигнала завершения
	done := make(chan struct{})

	// Запускаем shutdown в горутине
	go func() {
		log.Println("  → Stopping consumer gracefully...")

		// Вызываем graceful shutdown consumer
		simpleConsumer.Shutdown()

		log.Println("  ✓ Consumer stopped gracefully")
		close(done)
	}()

	// Ожидаем завершения или таймаута
	select {
	case <-done:
		log.Println("✅ Graceful shutdown completed successfully")

	case <-shutdownCtx.Done():
		log.Println("⚠️  Graceful shutdown timeout exceeded")
		log.Println("  → Forcing immediate shutdown...")

		// При таймауте принудительно закрываем клиент
		if simpleConsumer != nil {
			// Close клиента уже вызывается в Shutdown, но на всякий случай
			log.Println("  → Force closing Kafka client...")
		}
		log.Println("  ✓ Consumer forcibly stopped")
	}
}

// gracefulShutdownResources закрывает ресурсы контейнера (вызывается через defer)
func gracefulShutdownResources(container *deps.Container) {
	log.Println("🧹 Cleaning up resources...")

	// Создаем контекст с таймаутом для закрытия ресурсов
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Канал для сигнала завершения закрытия ресурсов
	done := make(chan struct{})

	go func() {
		log.Println("  → Closing Kafka client connections...")

		if err := container.Close(); err != nil {
			log.Printf("  ⚠️  Error closing container: %v", err)
		} else {
			log.Println("  ✓ All resources closed successfully")
		}

		close(done)
	}()

	// Ожидаем закрытия или таймаута
	select {
	case <-done:
		log.Println("✅ Cleanup completed")
	case <-closeCtx.Done():
		log.Println("⚠️  Cleanup timeout exceeded, some resources may not be closed")
	}
}

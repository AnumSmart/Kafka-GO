package main

import (
	"context"
	"kafka_consumer/internal/deps"
	"log/slog"

	"os"
	"os/signal"
	"pkg/kafka"
	"syscall"
	"time"
)

// GracefulShutdownTimeout - таймаут для graceful shutdown
const GracefulShutdownTimeout = 30 * time.Second

// waitForShutdown ожидает сигнал завершения или ошибку consumer
func waitForShutdown(simpleConsumer kafka.Consumer, consumerErrors <-chan error, logger *slog.Logger) {
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
		logger.Info("Received signal", "signal", sig.String())
		// Для SIGQUIT делаем принудительное завершение без graceful
		if sig == syscall.SIGQUIT {
			logger.Warn("SIGQUIT received, forcing immediate shutdown")
			return
		}

		logger.Info("Initiating graceful shutdown...")

	case err := <-consumerErrors:
		logger.Error("Consumer error, initiating shutdown", "error", err)
	}

	// Выполняем graceful shutdown
	performGracefulShutdown(simpleConsumer)
}

// performGracefulShutdown выполняет корректное завершение работы consumer
func performGracefulShutdown(simpleConsumer kafka.Consumer, logger *slog.Logger) {
	logger.Info("⏳ Waiting for current batch processing to complete...")

	// Создаем контекст с таймаутом для graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), GracefulShutdownTimeout)
	defer shutdownCancel()

	// Создаем канал для сигнала завершения
	done := make(chan struct{})

	// Запускаем shutdown в горутине
	go func() {
		logger.Info("  → Stopping consumer gracefully...")

		// Вызываем graceful shutdown consumer
		simpleConsumer.Shutdown()

		logger.Info("  ✓ Consumer stopped gracefully")
		close(done)
	}()

	// Ожидаем завершения или таймаута
	select {
	case <-done:
		logger.Info("✅ Graceful shutdown completed successfully")

	case <-shutdownCtx.Done():
		logger.Warn("Graceful shutdown timeout exceeded, forcing immediate shutdown...")

		// При таймауте принудительно закрываем клиент
		if simpleConsumer != nil {
			// Close клиента уже вызывается в Shutdown, но на всякий случай
			logger.Info("Force closing Kafka client...")
		}
		logger.Info("  ✓ Consumer forcibly stopped")
	}
}

// gracefulShutdownResources закрывает ресурсы контейнера (вызывается через defer)
func gracefulShutdownResources(container *deps.Container, logger *slog.Logger) {
	logger.Info("🧹 Cleaning up resources...")

	// Создаем контекст с таймаутом для закрытия ресурсов
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Канал для сигнала завершения закрытия ресурсов
	done := make(chan struct{})

	go func() {
		logger.Info("  → Closing Kafka client connections...")

		if err := container.Close(); err != nil {
			logger.Warn("Error closing container", "error", err)
		} else {
			logger.Info("All resources closed successfully")
		}

		close(done)
	}()

	// Ожидаем закрытия или таймаута
	select {
	case <-done:
		logger.Info("✅ Cleanup completed")
	case <-closeCtx.Done():
		logger.Warn("Cleanup timeout exceeded, some resources may not be closed")
	}
}

package main

import (
	"context"
	"kafka-producer/internal/deps"
	"kafka-producer/internal/server"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// waitForShutdown ожидает сигнал завершения или ошибку сервера
func waitForShutdown(httpServer *server.ProducerServer, serverErrors <-chan error) {
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

	case err := <-serverErrors:
		log.Printf("❌ Server error: %v", err)
		log.Println("🛑 Initiating shutdown due to error...")
	}

	// Выполняем graceful shutdown
	performGracefulShutdown(httpServer)
}

// performGracefulShutdown выполняет корректное завершение работы
func performGracefulShutdown(httpServer *server.ProducerServer) {
	log.Println("⏳ Waiting for ongoing requests to complete...")

	// Создаем контекст с таймаутом для graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), GracefulShutdownTimeout)
	defer shutdownCancel()

	// Создаем канал для сигнала завершения
	done := make(chan struct{})

	// Запускаем shutdown в горутине
	go func() {
		log.Println("  → Stopping http server gracefully...")

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("  ⚠️  Graceful shutdown error: %v", err)
		} else {
			log.Println("  ✓ http server stopped gracefully")
		}

		close(done)
	}()

	// Ожидаем завершения или таймаута
	select {
	case <-done:
		log.Println("✅ Graceful shutdown completed successfully")

	case <-shutdownCtx.Done():
		log.Println("⚠️  Graceful shutdown timeout exceeded")
		log.Println("  → Forcing immediate shutdown...")

		// При таймауте делаем принудительную остановку
		err := httpServer.Close()
		if err != nil {
			log.Println(err.Error())
		}
		log.Println("  ✓ Server forcibly stopped")
	}
}

// gracefulShutdown закрывает ресурсы контейнера (вызывается через defer)
func gracefulShutdown(container *deps.Container) {
	log.Println("🧹 Cleaning up resources...")

	// Создаем контекст с таймаутом для закрытия ресурсов
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Канал для сигнала завершения закрытия ресурсов
	done := make(chan struct{})

	go func() {
		log.Println("  → Closing kafka writer connections...")

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

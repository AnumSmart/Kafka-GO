package main

import (
	"fmt"
	"kafka-producer/internal/server"
	"log"
	"time"
)

// startGRPCServer запускает gRPC сервер в горутине
func startHTTPServer(httpServer *server.ProducerServer, serverErrors chan<- error) {
	log.Println("🚀 Starting http kafka-producer server...")

	// Небольшая задержка перед запуском (опционально)
	if ServerStartDelay > 0 {
		time.Sleep(ServerStartDelay)
	}

	// Запускаем сервер в горутине, чтобы не блокировать main
	go func() {
		log.Printf("✓ http server listening on port %s", httpServer.GetPort())
		log.Println("========================================")
		log.Println("Server is ready to accept requests")
		log.Println("========================================")

		// Run блокирует выполнение, пока сервер не остановится или не произойдет ошибка
		if err := httpServer.Run(); err != nil {
			serverErrors <- fmt.Errorf("http server error: %w", err)
		}
	}()
}

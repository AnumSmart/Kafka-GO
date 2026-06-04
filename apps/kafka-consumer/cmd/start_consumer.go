package main

import (
	"context"
	"kafka-consumer/internal/consumer"
	"log"
)

// startConsumer запускает consumer и возвращает канал ошибок
func startConsumer(ctx context.Context, simpleConsumer *consumer.SimpleConsumer) <-chan error {
	errorChan := make(chan error, 1)

	go func() {
		log.Println("🎧 Starting Kafka consumer...")

		// Запускаем основной цикл потребления
		if err := simpleConsumer.Start(ctx); err != nil {
			log.Printf("❌ Consumer error: %v", err)
			errorChan <- err
		}

		close(errorChan)
		log.Println("✅ Consumer finished")
	}()

	return errorChan
}

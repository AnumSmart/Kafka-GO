package main

import (
	"context"
	"log/slog"
	"pkg/kafka"
)

// startConsumer запускает consumer и возвращает канал ошибок
func startConsumer(ctx context.Context, simpleConsumer kafka.Consumer, logger *slog.Logger) <-chan error {
	errorChan := make(chan error, 1)

	go func() {
		logger.Info("Starting Kafka consumer...")

		// Запускаем основной цикл потребления
		if err := simpleConsumer.Start(ctx); err != nil {
			logger.Error("Consumer error", "error", err)
			errorChan <- err
		}

		close(errorChan)
		logger.Info("Consumer finished")
	}()

	return errorChan
}

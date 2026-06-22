package main

// Этот файл может быть пустым, так как deps.NewContainer уже существует
// Но для консистентности с producer, создадим его как обёртку

import (
	"context"
	"fmt"
	"kafka_consumer/internal/config"
	"kafka_consumer/internal/deps"
	"log/slog"
)

// NewDIContainer - фабричная функция для создания DI контейнера
// Возвращает контейнер и логгер для использования в main
func NewDIContainer(ctx context.Context, cfg *config.ConsumerServiceConfig) (*deps.Container, *slog.Logger, error) {
	container, err := deps.NewContainer(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	// Получаем логгер из контейнера
	logger := container.GetLogger()
	if logger == nil {
		return nil, nil, fmt.Errorf("Logger is not initialized!")
	}

	return container, logger, nil
}

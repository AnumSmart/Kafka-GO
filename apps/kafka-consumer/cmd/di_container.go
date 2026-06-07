package main

// Этот файл может быть пустым, так как deps.NewContainer уже существует
// Но для консистентности с producer, создадим его как обёртку

import (
	"context"
	"kafka_consumer/internal/config"
	"kafka_consumer/internal/deps"
)

// NewDIContainer - фабричная функция для создания DI контейнера
// (обёртка для удобства)
func NewDIContainer(ctx context.Context, cfg *config.ConsumerConfig) (*deps.Container, error) {
	return deps.NewContainer(ctx, cfg)
}

package order

import (
	"fmt"

	"github.com/google/uuid"
)

// generateEventID - генерирует уникальный ID события
func generateEventID() string {
	// Вариант: UUID (рекомендуется)
	return uuid.New().String()

	// Альтернатива: семантический ID для дебага
	// return fmt.Sprintf("%s:%s:%d", orderID, status, time.Now().UnixNano())
}

// IdempotencyKey - ключ идемпотентности для Redis
func (e *Event) IdempotencyKey() string {
	return fmt.Sprintf("idem:order:%s:event:%s", e.OrderID, e.EventID)
}

// PartitionKey - ключ для партиционирования (OrderID)
func (e *Event) PartitionKey() string {
	return e.OrderID
}

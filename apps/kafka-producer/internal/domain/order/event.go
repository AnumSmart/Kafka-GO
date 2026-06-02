package order

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event - доменное событие заказа
type Event struct {
	// Идентификаторы
	OrderID string `json:"order_id"` // Агрегат ID
	EventID string `json:"event_id"` // Событие ID (для идемпотентности)

	// Данные события
	Status    Status    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Payload   any       `json:"payload,omitempty"`

	// Метаданные
	Version int    `json:"version"`
	Source  string `json:"source"`

	// Внутренние поля (не сериализуются)
	errors map[string]string `json:"-"`
}

// NewEvent - фабрика событий
func NewEvent(orderID string, status Status, payload any) *Event {
	return &Event{
		OrderID:   orderID,
		EventID:   generateEventID(),
		Status:    status,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
		Version:   1,
		Source:    "kafka-producer",
		errors:    make(map[string]string),
	}
}

// Validate - валидация доменной логики
func (e *Event) Validate() error {
	e.errors = make(map[string]string)

	// Валидация OrderID
	if e.OrderID == "" {
		e.errors["order_id"] = "order_id is required"
	}

	// Валидация EventID
	if e.EventID == "" {
		e.errors["event_id"] = "event_id is required"
	}

	// Валидация статуса (используем методы Value Object)
	if !e.Status.IsValid() {
		e.errors["status"] = fmt.Sprintf("invalid status: %s", e.Status.String())
	}

	// Бизнес-правила
	if err := e.applyBusinessRules(); err != nil {
		e.errors["business"] = err.Error()
	}

	if len(e.errors) > 0 {
		return fmt.Errorf("validation failed: %v", e.errors)
	}

	return nil
}

// isValidStatus - проверка допустимости статуса
func (e *Event) isValidStatus() bool {
	switch e.Status {
	case StatusCreated, StatusPaid, StatusShipped, StatusDelivered, StatusCancelled:
		return true
	default:
		return false
	}
}

// applyBusinessRules - бизнес-правила домена
func (e *Event) applyBusinessRules() error {
	// Пример: нельзя отгрузить неоплаченный заказ
	if e.Status == StatusShipped {
		// Здесь может быть проверка, что заказ был оплачен
		// Но для этого нужно загружать агрегат из хранилища
		// В событии мы не можем этого сделать, только в сервисе
	}

	return nil
}

// MarshalJSON - кастомная сериализация
func (e *Event) MarshalJSON() ([]byte, error) {
	type Alias Event
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(e),
	})
}

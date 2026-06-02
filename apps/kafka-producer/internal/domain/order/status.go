package order

import (
	"encoding/json"
	"fmt"
)

// Status - value object для статуса заказа
type Status struct {
	value string
}

// Предопределённые статусы (константы домена)
var (
	StatusCreated   = Status{value: "created"}
	StatusPaid      = Status{value: "paid"}
	StatusShipped   = Status{value: "shipped"}
	StatusDelivered = Status{value: "delivered"}
	StatusCancelled = Status{value: "cancelled"}

	// Все допустимые статусы для валидации
	validStatuses = map[string]Status{
		"created":   StatusCreated,
		"paid":      StatusPaid,
		"shipped":   StatusShipped,
		"delivered": StatusDelivered,
		"cancelled": StatusCancelled,
	}
)

// NewStatus - фабрика для создания статуса из строки
func NewStatus(s string) (Status, error) {
	status, exists := validStatuses[s]
	if !exists {
		return Status{}, fmt.Errorf("invalid status: %s", s)
	}
	return status, nil
}

// String - возвращает строковое представление
func (s Status) String() string {
	return s.value
}

// IsValid - проверка валидности статуса
func (s Status) IsValid() bool {
	_, exists := validStatuses[s.value]
	return exists
}

// Equals - сравнение двух статусов
func (s Status) Equals(other Status) bool {
	return s.value == other.value
}

// CanTransitionTo - проверка возможности перехода в другой статус
func (s Status) CanTransitionTo(target Status) bool {
	// Бизнес-правила переходов между статусами
	transitions := map[Status][]Status{
		StatusCreated:   {StatusPaid, StatusCancelled},
		StatusPaid:      {StatusShipped, StatusCancelled},
		StatusShipped:   {StatusDelivered},
		StatusDelivered: {},
		StatusCancelled: {},
	}

	allowed, exists := transitions[s]
	if !exists {
		return false
	}

	for _, status := range allowed {
		if status.Equals(target) {
			return true
		}
	}

	return false
}

// MarshalJSON - кастомная сериализация для JSON
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.value)
}

// UnmarshalJSON - кастомная десериализация из JSON
func (s *Status) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	status, err := NewStatus(value)
	if err != nil {
		return err
	}

	s.value = status.value
	return nil
}

// AllStatuses - возвращает все возможные статусы
func AllStatuses() []Status {
	return []Status{
		StatusCreated,
		StatusPaid,
		StatusShipped,
		StatusDelivered,
		StatusCancelled,
	}
}

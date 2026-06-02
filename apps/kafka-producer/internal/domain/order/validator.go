package order

import (
	"fmt"
	"regexp"
)

// Validator - доменный валидатор
type Validator struct {
	errors map[string]string
}

// NewValidator - конструктор валидатора
func NewValidator() *Validator {
	return &Validator{
		errors: make(map[string]string),
	}
}

// ValidateOrderID - проверка формата OrderID
func (v *Validator) ValidateOrderID(orderID string) {
	if orderID == "" {
		v.errors["order_id"] = "order_id cannot be empty"
		return
	}

	// Пример бизнес-правила: формат "ORD-XXXXXX"
	matched, _ := regexp.MatchString(`^ORD-[A-Z0-9]{6}$`, orderID)
	if !matched {
		v.errors["order_id"] = "order_id must match format ORD-XXXXXX"
	}
}

// ValidateStatusTransition - проверка допустимости перехода статусов
func (v *Validator) ValidateStatusTransition(currentStatus, newStatus Status) {
	// Бизнес-правила переходов
	allowedTransitions := map[Status][]Status{
		StatusCreated:   {StatusPaid, StatusCancelled},
		StatusPaid:      {StatusShipped, StatusCancelled},
		StatusShipped:   {StatusDelivered},
		StatusDelivered: {},
		StatusCancelled: {},
	}

	allowed, exists := allowedTransitions[currentStatus]
	if !exists {
		v.errors["status"] = fmt.Sprintf("unknown current status: %s", currentStatus)
		return
	}

	for _, s := range allowed {
		if s == newStatus {
			return
		}
	}

	v.errors["status"] = fmt.Sprintf("invalid transition from %s to %s", currentStatus, newStatus)
}

// HasErrors - есть ли ошибки валидации
func (v *Validator) HasErrors() bool {
	return len(v.errors) > 0
}

// Errors - получить все ошибки
func (v *Validator) Errors() map[string]string {
	return v.errors
}

// Error - реализация интерфейса error
func (v *Validator) Error() string {
	return fmt.Sprintf("validation failed: %v", v.errors)
}

package handlers

import (
	"context"
	"log"
	"net/http"
	"time"

	"kafka-producer/internal/domain/order"

	"github.com/gin-gonic/gin"
)

// ==================== CREATE ORDER ====================

type CreateOrderRequest struct {
	OrderID string `json:"order_id" binding:"required"`
	Payload any    `json:"payload,omitempty"`
}

type CreateOrderResponse struct {
	Status  string `json:"status"`
	EventID string `json:"event_id"`
	OrderID string `json:"order_id"`
	Message string `json:"message,omitempty"`
}

// CreateOrder - создание нового заказа (отправляем событие "created")
// CreateOrder - создание нового заказа
func (h *Handler) CreateOrder(c *gin.Context) {
	var req CreateOrderRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, CreateOrderResponse{
			Status:  "error",
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// ПРАВИЛЬНО: используем предопределённую константу
	// Не нужно вызывать NewStatus, StatusCreated уже готовый value object
	event := order.NewEvent(req.OrderID, order.StatusCreated, req.Payload)

	// Валидация события
	if err := event.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, CreateOrderResponse{
			Status:  "error",
			Message: "Invalid event: " + err.Error(),
		})
		return
	}

	// Отправляем в Kafka
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := h.kafkaProducer.SendOrderEvent(ctx, event); err != nil {
		log.Printf("Kafka sending message error:%v", err.Error())
		c.JSON(http.StatusInternalServerError, CreateOrderResponse{
			Status:  "error",
			Message: "Failed to send event to Kafka: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, CreateOrderResponse{
		Status:  "success",
		EventID: event.EventID,
		OrderID: event.OrderID,
		Message: "Order created event sent successfully",
	})
}

// ==================== UPDATE ORDER STATUS ====================

type UpdateOrderStatusRequest struct {
	OrderID string `json:"order_id" binding:"required"`
	Status  string `json:"status" binding:"required"`
	Payload any    `json:"payload,omitempty"`
}

type UpdateOrderStatusResponse struct {
	Status    string `json:"status"`
	EventID   string `json:"event_id"`
	OrderID   string `json:"order_id"`
	NewStatus string `json:"new_status"`
	Message   string `json:"message,omitempty"`
}

// UpdateOrderStatus - обновление статуса заказа
func (h *Handler) UpdateOrderStatus(c *gin.Context) {
	var req UpdateOrderStatusRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, UpdateOrderStatusResponse{
			Status:  "error",
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	// ПРАВИЛЬНО: конвертируем строку из запроса в value object
	// Здесь req.Status - это строка от клиента ("paid", "shipped" и т.д.)
	newStatus, err := order.NewStatus(req.Status)
	if err != nil {
		c.JSON(http.StatusBadRequest, UpdateOrderStatusResponse{
			Status:  "error",
			Message: err.Error(),
		})
		return
	}

	// Создаём событие с полученным статусом
	event := order.NewEvent(req.OrderID, newStatus, req.Payload)

	if err := event.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, UpdateOrderStatusResponse{
			Status:  "error",
			Message: "Invalid event: " + err.Error(),
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := h.kafkaProducer.SendOrderEvent(ctx, event); err != nil {
		c.JSON(http.StatusInternalServerError, UpdateOrderStatusResponse{
			Status:  "error",
			Message: "Failed to send event to Kafka: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, UpdateOrderStatusResponse{
		Status:    "success",
		EventID:   event.EventID,
		OrderID:   event.OrderID,
		NewStatus: newStatus.String(),
		Message:   "Order status update event sent successfully",
	})
}

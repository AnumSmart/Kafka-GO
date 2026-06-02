package handlers

import (
	kafkalayer "kafka-producer/internal/kafka_layer"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Хэндлер для продьюссера
type Handler struct {
	kafkaProducer kafkalayer.ProducerInterface
}

// конструктор для создания хэндлера
func NewHandler(producer kafkalayer.ProducerInterface) *Handler {
	return &Handler{
		kafkaProducer: producer,
	}
}

// Создаём ЭХО-метод для тестирвоания
func (h *Handler) Echo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "Hello from producer server!"})
}

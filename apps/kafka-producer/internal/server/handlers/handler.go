package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Хэндлер для продьюссера
type Handler struct{}

// конструктор для создания хэндлера
func NewHandler() *Handler {
	return &Handler{}
}

// Создаём ЭХО-метод для тестирвоания
func (h *Handler) Echo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "Hello from producer server!"})
}

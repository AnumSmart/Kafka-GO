package server

import (
	"context"
	"kafka-producer/internal/server/handlers"
	"log"
	"net/http"
	"pkg/configs"

	"github.com/gin-gonic/gin"
)

// структура сервера для продьюссера
type ProducerServer struct {
	httpServer *http.Server
	router     *gin.Engine
	config     *configs.ServerConfig
	Handler    *handlers.Handler
}

// Конструктор для сервера - продьюссера
func NewProducerServer(ctx context.Context, config *configs.ServerConfig, handler *handlers.Handler) (*ProducerServer, error) {
	// создаём экземпляр роутера
	router := gin.Default()
	err := router.SetTrustedProxies(nil)
	if err != nil {
		return nil, err
	}

	return &ProducerServer{
		router:  router,
		config:  config,
		Handler: handler,
	}, nil
}

// Метод для маршрутизации сервера
func (a *ProducerServer) SetUpRoutes() {
	a.router.GET("/hello", a.Handler.Echo) // тестовый ендпоинт
}

// Метод для запуска сервера
func (a *ProducerServer) Run() error {
	a.SetUpRoutes()

	a.httpServer = &http.Server{
		Handler: a.router,
	}
	// Используем обычный порт для HTTP
	a.httpServer.Addr = a.config.Addr()
	log.Printf("Starting HTTP server on %s", a.config.Addr())
	return a.httpServer.ListenAndServe()
}

// Метод для graceful shutdown
func (a *ProducerServer) Shutdown(ctx context.Context) error {

	// Останавливаем HTTP сервер
	if err := a.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	log.Println("Server shutdown completed")
	return nil
}

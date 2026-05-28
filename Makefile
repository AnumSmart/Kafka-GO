# Переменные
DOCKER_COMPOSE = docker-compose
COMPOSE_KAFKA_FILE = deployments/kafka/docker-compose.yml
KAFKA_ENV_FILE = deployments/kafka/.env
KAFKA_COMPOSE_CMD = $(DOCKER_COMPOSE) -f $(COMPOSE_KAFKA_FILE) --env-file $(KAFKA_ENV_FILE)

# Запуск контейнера для kafka и kafkaUI
.PHONY: kafka_up
kafka_up:
	@echo "[INFO] Starting Kafka and KafkaUI containers..."
	$(KAFKA_COMPOSE_CMD) up -d
	@echo "[OK] Kafka Container started"
	@make --no-print-directory kafka_container_status

# Остановка контейнера для kafka и kafkaUI
.PHONY: kafka_down
kafka_down:
	@echo "[INFO] Stopping Kafka container..."
	$(KAFKA_COMPOSE_CMD) down
	@echo "[OK] Kafka Container stopped"

# Статус контейнера для rabbitMQ
.PHONY: kafka_status
kafka_status:
	@echo "[INFO] Container status:"
	$(KAFKA_COMPOSE_CMD) ps

# Показать все доступные команды
.PHONY: help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Main commands:"
	@echo "  make kafka_up              - Start all containers (kafka + kafkaUI)"
	@echo "  make kafka_down            - Stop all containers (kafka + kafkaUI)"
	@echo "  make kafka_status          - Status for all containers (kafka + kafkaUI)"
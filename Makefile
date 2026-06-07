# Переменные
DOCKER_COMPOSE = docker-compose
COMPOSE_KAFKA_FILE = deployments/kafka/docker-compose.yml
KAFKA_ENV_FILE = deployments/kafka/.env
CONSUMER_REDIS_ENV_FILE = deployments/consumer_redis/.env
COMPOSE_CONSUMER_REDIS_FILE = deployments/consumer_redis/docker-compose.yml
KAFKA_COMPOSE_CMD = $(DOCKER_COMPOSE) -f $(COMPOSE_KAFKA_FILE) --env-file $(KAFKA_ENV_FILE)
CONSUMER_REDIS_COMPOSE_CMD = $(DOCKER_COMPOSE) -f $(COMPOSE_CONSUMER_REDIS_FILE) --env-file $(CONSUMER_REDIS_ENV_FILE)

# Запуск контейнера для kafka и kafkaUI
.PHONY: kafka_up
kafka_up:
	@echo "[INFO] Starting Kafka and KafkaUI containers..."
	$(KAFKA_COMPOSE_CMD) up -d
	@echo "[OK] Kafka Container started"
	@make --no-print-directory kafka_status

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

# Запуск контейнера для консьюмер редиса (идемпотентные ключи)
.PHONY: consumer_redis_up
consumer_redis_up:
	@echo "[INFO] Starting Consumer Redis container"
	$(CONSUMER_REDIS_COMPOSE_CMD) up -d
	@echo "[OK] Consumer Redis Container started"
	@make --no-print-directory consumer_redis_status

# Остановка контейнера для консьюмер редиса (идемпотентные ключи)
.PHONY: consumer_redis_down
consumer_redis_down:
	@echo "[INFO] Stopping Consumer Redis container..."
	$(CONSUMER_REDIS_COMPOSE_CMD) down
	@echo "[OK] Consumer Redis Container stopped"

# Статус контейнера для consumer redis
.PHONY: consumer_redis_status
consumer_redis_status:
	@echo "[INFO] Container status:"
	$(CONSUMER_REDIS_COMPOSE_CMD) ps

# Показать все доступные команды
.PHONY: help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Main commands:"
	@echo "  make kafka_up                 - Start all containers (kafka + kafkaUI)"
	@echo "  make kafka_down               - Stop all containers (kafka + kafkaUI)"
	@echo "  make kafka_status             - Status for all containers (kafka + kafkaUI)"
	@echo "  make consumer_redis_up        - Start consumer redis container"
	@echo "  make consumer_redis_down      - Stop consumer redis container"
	@echo "  make consumer_redis_status    - Status for consumer redic container"
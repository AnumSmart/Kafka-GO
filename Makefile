# Переменные
DOCKER_COMPOSE = docker-compose
COMPOSE_KAFKA_FILE = deployments/kafka/docker-compose.yml
KAFKA_ENV_FILE = deployments/kafka/.env
CONSUMER_REDIS_ENV_FILE = deployments/consumer_redis/.env
COMPOSE_CONSUMER_REDIS_FILE = deployments/consumer_redis/docker-compose.yml
KAFKA_COMPOSE_CMD = $(DOCKER_COMPOSE) -f $(COMPOSE_KAFKA_FILE) --env-file $(KAFKA_ENV_FILE)
CONSUMER_REDIS_COMPOSE_CMD = $(DOCKER_COMPOSE) -f $(COMPOSE_CONSUMER_REDIS_FILE) --env-file $(CONSUMER_REDIS_ENV_FILE)

# Настройки линтера
GOLANGCI_LINT = golangci-lint
LINT_TIMEOUT = 5m
LINT_CONFIG = .golangci.yml

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

# ============================================================
# КОМАНДЫ ДЛЯ ЛИНТЕРА
# ============================================================

# Запуск линтера для всего проекта
.PHONY: lint
lint:
	@echo "[INFO] Running linter..."
	$(GOLANGCI_LINT) run --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) ./...
	@echo "[OK] Linter finished successfully"

# Запуск линтера только для быстрых проверок (экономит время)
.PHONY: lint-fast
lint-fast:
	@echo "[INFO] Running linter (fast mode)..."
	$(GOLANGCI_LINT) run --fast --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) ./...
	@echo "[OK] Linter finished successfully"

# Запуск линтера и автоматическое исправление проблем
.PHONY: lint-fix
lint-fix:
	@echo "[INFO] Running linter with auto-fix..."
	$(GOLANGCI_LINT) run --fix --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) ./...
	@echo "[OK] Linter finished with fixes applied"

# Запуск линтера для конкретного пакета (нужно передать PACKAGE=./pkg/...)
.PHONY: lint-pkg
lint-pkg:
	@if [ -z "$(PACKAGE)" ]; then \
		echo "[ERROR] Please specify PACKAGE, e.g.: make lint-pkg PACKAGE=./pkg/..."; \
		exit 1; \
	fi
	@echo "[INFO] Running linter for package: $(PACKAGE)"
	$(GOLANGCI_LINT) run --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) $(PACKAGE)
	@echo "[OK] Linter finished for package: $(PACKAGE)"

# Запуск линтера для конкретного файла (нужно передать FILE=./path/to/file.go)
.PHONY: lint-file
lint-file:
	@if [ -z "$(FILE)" ]; then \
		echo "[ERROR] Please specify FILE, e.g.: make lint-file FILE=./pkg/main.go"; \
		exit 1; \
	fi
	@echo "[INFO] Running linter for file: $(FILE)"
	$(GOLANGCI_LINT) run --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) $(FILE)
	@echo "[OK] Linter finished for file: $(FILE)"

# Запуск линтера с выводом всех ошибок (включая те, что обычно игнорируются)
.PHONY: lint-verbose
lint-verbose:
	@echo "[INFO] Running linter (verbose mode)..."
	$(GOLANGCI_LINT) run -v --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) ./...
	@echo "[OK] Linter finished"

# Проверка наличия golangci-lint и его установка, если отсутствует
.PHONY: lint-install
lint-install:
	@echo "[INFO] Checking golangci-lint installation..."
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 || { \
		echo "[WARN] golangci-lint not found. Installing..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
		echo "[OK] golangci-lint installed"; \
	}
	@echo "[OK] golangci-lint is ready"

# Показать доступные линтеры
.PHONY: lint-list
lint-list:
	@echo "[INFO] Available linters:"
	$(GOLANGCI_LINT) linters

# ============================================================
# КОМАНДА ДЛЯ ПРОВЕРКИ ВСЕГО (линтер + тесты)
# ============================================================

# Запуск линтера и тестов (для CI/CD)
.PHONY: ci-check
ci-check: lint test
	@echo "[OK] All checks passed"

# Запуск тестов (пример)
.PHONY: test
test:
	@echo "[INFO] Running tests..."
	go test -v -race -coverprofile=coverage.out ./...
	@echo "[OK] Tests finished"

# Показать все доступные команды
.PHONY: help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Main commands:"
	@echo "  make kafka_up                      - Start all containers (kafka + kafkaUI)"
	@echo "  make kafka_down                    - Stop all containers (kafka + kafkaUI)"
	@echo "  make kafka_status                  - Status for all containers (kafka + kafkaUI)"
	@echo "  make consumer_redis_up             - Start consumer redis container"
	@echo "  make consumer_redis_down           - Stop consumer redis container"
	@echo "  make consumer_redis_status         - Status for consumer redis container"
	@echo ""
	@echo "Linter commands:"
	@echo "  make lint                          - Run linter for entire project"
	@echo "  make lint-fast                     - Run linter in fast mode (faster checks)"
	@echo "  make lint-fix                      - Run linter and auto-fix issues"
	@echo "  make lint-pkg PACKAGE=./pkg/...    - Run linter for specific package"
	@echo "  make lint-file FILE=./file.go      - Run linter for specific file"
	@echo "  make lint-verbose                  - Run linter with verbose output"
	@echo "  make lint-install                  - Install golangci-lint if missing"
	@echo "  make lint-list                     - Show available linters"
	@echo ""
	@echo "CI/CD commands:"
	@echo "  make ci-check                      - Run linter and tests (for CI)"
	@echo "  make test                          - Run all tests with coverage"
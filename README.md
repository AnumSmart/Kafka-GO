# KafkaGo

Work with Kafka using [kafka-go](https://github.com/segmentio/kafka-go) library (producer).
Work with Kafka using [franz-go](https://github.com/twmb/franz-go/pkg/kgo) library (consumer).

## 📋 Требования

- Go 1.21+
- Docker & Docker Compose
- Make (опционально)

## 📁 Структура проекта

```
KAFKAGO/
├── apps/                               # сервисы с простым использованием kafka
│ ├── kafka-consumer/                   # Сервис консьюмера
│ └── kafka-producer/                   # Сервис продьюссера
├── deployments/                        # Docker Compose
├── global_models/                      # глобальные модели
│ ├── kafka/                            # Развертывание Kafka
│ |     ├── .env
│ |     ├── .env.example
│ |     └── docker-compose.yml
├── pkg/                                # общие библиотеки
│ ├── configs/                          # логика конфигов
│ ├── kafka/                            # логика создания kafka.reader и kafka.wtiter
│ ├── logger/                           # логика создания логгера slog
│ ├── redis/                            # логика создания кэша (redis)
│ └── go.mod                            # зависимости для этого пакета
├── Makefile
├── .mockery.yml
├── go.work
├── .gitignore
└── README.md
```

### Доступные команды make

# Из корня проекта

make help

# KafkaGo

Work with Kafka using [kafka-go](https://github.com/segmentio/kafka-go) library.

## 📋 Требования

- Go 1.21+
- Docker & Docker Compose
- Make (опционально)

## 📁 Структура проекта

```
KAFKAGO/
├── apps/                               # сервисы с простым использованием kafka
├── deployments/                        # Docker Compose
│ ├── kafka/                            # Развертывание Kafka
│ |     ├── .env
│ |     ├── .env.example
│ |     └── docker-compose.yml
├── pkg/                                # общие библиотеки
│ ├── configs/                          # логика конфигов
│ └── go.mod                            # зависимости для этого пакета
├── Makefile
├── go.work
├── .gitignore
└── README.md
```

### Доступные команды make

# Из корня проекта

make help

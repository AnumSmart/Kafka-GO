# pkg

Описание логики для переиспользуемых пакетов

## 📁 Структура проекта

```
├── pkg/                                    # переиспользуемая логика
│ ├── configs/                              # логика конфигов
│ |     ├── config_loader_yml_test.go       # тесты для configLoader
│ |     ├── config_loader_yml.go            # Универсальный загрузчик конфигов из yml (дженерик)
│ |     ├── consumer_config.go              # конфиг для консъюмера (на библиотеке kafka-go, segmentio)
│ |     ├── consumer_config_farnz.go        # конфиг для консъюмера (на библиотеке franz-go)
│ |     ├── producer_config.go              # конфиг для продьюссера
│ |     ├── redis_config_test.go            # тесты конфига редиса
│ |     ├── redis_config.go                 # конфиг для редиса
│ |     ├── kafka_config.go                 # конфиг для кафки
│ |     ├── tools_test.go                   # тесты для вспомогательных функций
│ |     ├── tools.go                        # вспомогательные функции
│ |     └── server_config.go                # конфиг для http сервера
│ ├── kafka/                                # логика для кафки
│ |     ├── franz-go-consumer/              # логика базового консьюмера (библиотека franz-go)
│ |     ├── franz-go-producer/              # логика базового продьюссера (библиотека franz-go)
│ |     ├── kafka-go-segm-producer/         # создаём kafka writer (библиотека kafka-go, segmentio)
│ |     |       └── producer.go             # логика продьюссера
│ |     ├── errors.go                       # общие ошибки для пакета kafka
│ |     ├── interfaces.go                   # общие интерфейсы для пакета kafka
│ |     └── models.go                       # общие модели для пакета kafka
│ ├── logger/                               # логика создания логгера slog
│ |     ├── logger_config.go                # логика создания конфига для slog
│ |     └── logger.go                       # логика slog
│ ├── redis/                                # логика создания экземпляра редис
│ |     ├── cache_redis_adapter.go          # адаптер для соответствия глобальному интерфейсу
│ |     └── redis.go                        # создаём репозиторий для редиса
│ ├── go.mod
│ ├── go.sum
│ └── README.md                             # ридми
```

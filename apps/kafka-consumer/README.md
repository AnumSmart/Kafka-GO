# Kafka Consumer на franz-go

## 📦 Используемые библиотеки

| Библиотека                                   | Версия | Назначение              |
| -------------------------------------------- | ------ | ----------------------- |
| [franz-go](https://github.com/twmb/franz-go) | latest | Kafka клиент (consumer) |
| [godotenv](https://github.com/joho/godotenv) | v1.5+  | Загрузка .env файлов    |
| [gopkg.in/yaml.v3](https://gopkg.in/yaml.v3) | v3     | Парсинг YAML конфигов   |

## 📁 Структура пакета

```
├── apps/                                        # сервисы с простым использованием kafka
│ ├── kafka-consumer/                            # консъюмер
│ |     ├── cmd/                                 # точка входа
│ |     |    ├── main.go
│ |     |    ├── di_container.go                 # создание контейнера
│ |     |    ├── grace_shut_down.go              # логика gracefull shutdown
│ |     |    └── start_consumer.go               # логика запуска консьюмера
│ |     ├── configs-yml/                         # конфиги в yml формате
│ |     |    └── kafkaClientConfig.yml           # параметры kafka client (совмещает продьюссера и консьюммера)
│ |     ├── internal/                            # внутренняя логика
│ |     |    ├── config/                         # готовые конфиги (на базе pkg и yml файлов)
│ |     |    |     └── consumer_config.go        # парсинг конфига
│ |     |    └── consumer/                       # описание консьюмера
│ |     |    |     ├── simple_consumer.go        # основной цикл потребления
│ |     |    |     ├── store_handler.go          # Обработчик сообщений с проверкой иденмпотентности
│ |     |    |     └── message_store.go          # Хранилище сообщений (слайс)
│ |     |    ├── deps/                           # логика работы DI
│ |     |    |     ├── di.go                     # DI - контейнер
│ |     |    |     └── di_methods.go             # методы DI контейнера
│ |     |    ├── idempotency/                    # кэш для идемпотентности
│ |     |    |     └── redis_cache.go            # проверка/сохранение EventID в Redis
│ |     ├── .env                                 # env для консьюмера
│ |     └── go.mod                               # зависимости
```

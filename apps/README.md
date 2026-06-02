# apps

Логика работы 2х сервисов (consumer и producer)

## 📁 Структура проекта

```
├── apps/                                        # сервисы с простым использованием kafka
│ ├── kafka-consumer/                            # сервер - консъюмер
│ |     ├── cmd/                                 # точка входа
│ |     |    └── main.go
│ |     ├── configs-yml/                         # конфиги в yml формате
│ |     |    ├── consumer.yml                    # параметры для сервера consumer
│ |     |    └── serverConfig.yml                # конфиг для http сервера
│ |     ├── internal/                            # внутренняя логика
│ |     |    ├── config/                         # готовые конфиги (на базе pkg и yml файлов)
│ |     |    |     └── consumer_config.go        # конфиг консьюмера
│ |     |    └── server/                         # описание сервера
│ |     ├── .env                                 # env для консьюмера
│ |     └── go.mod                               # зависимости
│ ├── kafka-producer/                            # сервер - продьюссер
│ |     ├── cmd/                                 # точка входа
│ |     |    ├── main.go
│ |     |    ├── di_container.go                 # создание контейнера
│ |     |    ├── grace_shut_down.go              # логика gracefull shutdown
│ |     |    ├── start_server.go                 # логика запуска сервера
│ |     ├── configs-yml/                         # конфиги в yml формате
│ |     |    ├── producerConfig.yml              # конфиг продьюссера
│ |     |    └── serverConfig.yml                # конфиг для http сервера
│ |     ├── internal/                            # внутренняя логика
│ |     |    ├── config/                         # готовые конфиги (на базе pkg и yml файлов)
│ |     |    |     └── producer_config.go        # конфиг продьюссера
│ |     |    ├── server/                         # описание сервера
│ |     |    |     ├── handlers/                 # логика работы хэндлеров
│ |     |    |     └── server.go                 # логика работы сервера
│ |     |    ├── deps/                           # логика работы DI
│ |     |    |     ├── di.go                     # DI - контейнер
│ |     |    |     └── di_methods.go             # методы DI контейнера
│ |     |    ├── domain/                         # доменные модели
│ |     |    |     └──order                      # модель заказов
│ |     |    |         ├── event.go              # агрегат/событие
│ |     |    |         ├── id.go                 # генерация ID
│ |     |    |         ├── status.go             # value object
│ |     |    |         └── validator.go          # доменные правила
│ |     |    └── kafka_layer/                    # отдельный слой работы с кафкой
│ |     |    |     └── producer.go               # работа с продьюссером
│ |     ├── .env                                 # env для провьюссера
│ |     └── go.mod                               # зависимости
│ └── README.md                                  # ридми
```

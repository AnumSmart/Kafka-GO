# pkg

Описание логики для переиспользуемых пакетов

## 📁 Структура проекта

```
├── pkg/                                    # переиспользуемая логика
│ ├── configs/                              # логика конфигов
│ |     ├── config_loader_yml_test.go       # тесты для configLoader
│ |     ├── config_loader_yml.go            # Универсальный загрузчик конфигов из yml (дженерик)
│ |     ├── consumer_config.go              # конфиг для консъюмера
│ |     ├── producer_config.go              # конфиг для продьюссера
│ |     ├── kafka_config.go                 # конфиг для кафки
│ |     └── server_config.go                # конфиг для http сервера
│ ├── go.mod
│ ├── go.sum
│ └── README.md                             # ридми
```

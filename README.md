# MCP APM Server

Минимальный сервер для безопасного локального доступа к APM логам Skyeng Platform через Elasticsearch API.

## Возможности

- 🔍 Поиск логов по произвольным фильтрам
- 📋 Получение схемы полей и событий индекса (из index.yaml)
- 🔒 Безопасность: локальное хранение кредов, только чтение данных
- 📑 Логирование всех операций
- 🚀 Кроссплатформенная установка - работает на Linux, macOS и Windows

## Быстрая установка

**Для всех операционных систем:**
```bash
# Клонируем репозиторий в любую директорию
git clone <repository-url> mcp-smartroom-apm
cd mcp-smartroom-apm

# Автоматическая установка зависимостей
python3 setup.py    # Linux/macOS
python setup.py     # Windows
```

Скрипт установки автоматически:
- Определит операционную систему
- Создаст виртуальное окружение
- Установит все зависимости
- Создаст скрипты запуска для вашей ОС

## Ручная установка

```bash
cd /path/to/your/projects
git clone <repository-url> mcp-smartroom-apm
cd mcp-smartroom-apm

# Создание виртуального окружения и установка зависимостей
python3 -m venv venv                    # Linux/macOS
python -m venv venv                     # Windows

# Активация виртуального окружения
source venv/bin/activate                # Linux/macOS
venv\Scripts\activate                   # Windows

pip install -r requirements.txt

# Настройка переменных окружения
cp .env.example .env
nano .env                               # Linux/macOS
notepad .env                            # Windows
```

Пример содержимого `.env`:
```bash
APM_BASE_URL=https://apm.skyeng.link
APM_USERNAME=your_username
APM_PASSWORD=your_password
APM_TIMEOUT=30
```

## Запуск сервера

**Linux/macOS:**
```bash
python3 mcp-server
# или после chmod +x mcp-server
./mcp-server
```

**Windows:**
```batch
python mcp-server
# или используйте созданный bat-файл
start_server.bat
```

**Дополнительные опции:**
```bash
python3 mcp-server --help    # показать справку
```

**Преимущества кроссплатформенного подхода:**
- ✅ Работает на Linux, macOS и Windows
- ✅ Автоматическая адаптация под ОС
- ✅ Понятные сообщения об ошибках
- ✅ Не требует bash или PowerShell

## Использование

### 1. Получить список индексов, полей и событий

**GET /v1/indexes**

Ответ:
```json
[
  {
    "name": "logs_videocall",
    "fields": {
      "appSessionId": "Уникальный ID сессии приложения, хэш комнаты",
      "event": "Тип события: tech-summary-minute",
      "timestamp": "Время события",
      "userId": "ID пользователя"
    },
    "events": [
      {"name": "tech-summary-minute", "description": "отчет по МОС за минуту"},
      {"name": "webrtcIssue", "description": "проблемы с WebRTC, аудио, видео, скриншеринг"}
    ]
  }
]
```

### 2. Поиск логов по индексу

**POST /v1/query/{index}**

Тело запроса (пример):
```json
{
  "filters": {
    "appSessionId": "abc123def456",
    "event": "tech-summary-minute",
    "timestamp": {"gte": "2024-01-15T10:00:00", "lte": "2024-01-15T11:00:00"}
  },
  "size": 10,
  "from_": 0,
  "sort": [{"@timestamp": "desc"}]
}
```

- `filters` — словарь фильтров (term или range)
- `size` — количество результатов (по умолчанию 100, максимум 100)
- `from_` — смещение (пагинация)
- `sort` — сортировка (опционально)

Ответ: стандартный ответ Elasticsearch

### Пример запроса через curl

```bash
curl -X GET http://localhost:8000/v1/indexes

curl -X POST http://localhost:8000/v1/query/logs_videocall \
  -H 'Content-Type: application/json' \
  -d '{"filters": {"event": "tech-summary-minute"}, "size": 5}'
```

## Архитектура

```
mcp-smartroom-apm/
├── mcp-apm-server.py         # Основной MCP сервер
├── mcp-server               # Кроссплатформенный скрипт запуска
├── setup.py                 # Скрипт автоматической установки
├── start_server.bat         # Windows bat-файл (создается на Windows)
├── index.yaml               # Описание индексов, полей и событий
├── requirements.txt         # Зависимости Python
├── .env                     # Креды (создается вручную)
├── .env.example             # Пример конфигурации
└── README.md                # Этот файл
```

## Логирование

- Все операции пишутся в `mcp-apm-server.log`
- Логируются: время, успешные/неуспешные операции, ошибки подключения и аутентификации
- Не логируются: пароли и чувствительные данные

## Устранение неполадок

- **401** — ошибка аутентификации: проверьте креды в .env
- **404** — индекс не найден: проверьте название индекса в index.yaml
- **503** — ошибка соединения: проверьте APM_BASE_URL и сеть

## Требования

- Python 3.8+
- Доступ к APM Skyeng Platform

## Зависимости

- fastapi
- uvicorn
- python-dotenv
- pyyaml
- elasticsearch
- httpx

## Лицензия

Внутренний инструмент Skyeng Platform 
#!/usr/bin/env python3
"""
MCP сервер для безопасной работы с Elasticsearch (APM)
Рефакторенная версия с разделением на модули
"""

import os
import logging
import json
import asyncio
from typing import List
from datetime import datetime, timedelta
from dotenv import load_dotenv
from mcp.server import Server
from mcp.types import Tool, TextContent
import mcp.server.stdio

from src.config_utils import load_index_config
from src.elasticsearch_client import ElasticsearchManager
from src.data_processing import process_elasticsearch_data
from src.plotting import PlotManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp-apm-server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Загружаем конфигурацию
INDEX_CONFIG = load_index_config()

# Инициализируем менеджеры
es_manager = ElasticsearchManager()
plot_manager = PlotManager()
server = Server("apm-server")

def get_data_retention_info():
    """Получить информацию о доступном периоде данных"""
    now = datetime.now()
    retention_days = 20
    oldest_available = now - timedelta(days=retention_days)
    return f"ВАЖНО: логи хранятся не более {retention_days} дней. Данные доступны с {oldest_available.strftime('%Y-%m-%d')} по {now.strftime('%Y-%m-%d')}. Поиск данных старше этого периода не даст результатов"

@server.list_tools()
async def list_tools() -> list[Tool]:
    """Список доступных инструментов MCP"""
    
    tools = [
        Tool(
            name="list_indexes",
            description="Получить список доступных индексов и их описание",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="get_data_retention_info",
            description="Получить информацию о доступном периоде данных в Elasticsearch. ВАЖНО: логи хранятся не более 20 дней. Если наобходимо получить данные по дате, то сначала надо получить информацию о доступном периоде данных",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="query_index",
            description="Выполнить запрос к индексу Elasticsearch с поддержкой стандартных Elasticsearch запросов",
            inputSchema={
                "type": "object",
                "properties": {
                    "index": {"type": "string", "description": "Имя индекса"},
                    "filters": {
                        "type": "object", 
                        "description": "Фильтры запроса в формате Elasticsearch. Поддерживаются: match_phrase (для поиска фраз), match (для поиска слов), term/terms (точное совпадение), range (диапазоны), wildcard (с *, но лучше использовать .keyword поля), bool (комбинированные запросы), exists, prefix, fuzzy, regexp. Примеры: {\"match_phrase\": {\"message\": \"User has been notified\"}}, {\"range\": {\"@timestamp\": {\"gte\": \"now-1d\"}}}, {\"bool\": {\"must\": [{\"match_phrase\": {\"message\": \"error\"}}, {\"range\": {\"@timestamp\": {\"gte\": \"now-3h\"}}}]}}"
                    },
                    "size": {"type": "integer", "description": "Размер выборки", "default": 100},
                    "from_": {"type": "integer", "description": "Смещение", "default": 0},
                    "sort": {"type": "array", "items": {"type": "object"}, "description": "Сортировка"}
                },
                "required": ["index", "filters"]
            }
        )
    ]
    
    if plot_manager.is_available():
        tools.append(
            Tool(
                name="create_plot",
                description="Создать график по данным из Elasticsearch",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "index": {"type": "string", "description": "Имя индекса"},
                        "filters": {"type": "object", "description": "Фильтры запроса"},
                        "plot_type": {
                            "type": "string", 
                            "enum": ["line", "scatter", "bar", "mos_timeline", "metrics_comparison"],
                            "description": "Тип графика"
                        },
                        "x_field": {"type": "string", "description": "Поле для оси X (обычно @timestamp)"},
                        "y_field": {"type": "string", "description": "Поле для оси Y"},
                        "group_by": {"type": "string", "description": "Поле для группировки (например, userId)", "default": None},
                        "title": {"type": "string", "description": "Заголовок графика", "default": "График"},
                        "size": {"type": "integer", "description": "Размер выборки", "default": 100}
                    },
                    "required": ["index", "filters", "plot_type", "x_field", "y_field"]
                }
            )
        )
    
    return tools

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Обработчик вызовов инструментов MCP"""
    try:
        if name == "list_indexes":
            result = await es_manager.list_indexes(INDEX_CONFIG)
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
            
        elif name == "get_data_retention_info":
            return [TextContent(type="text", text=get_data_retention_info())]
            
        elif name == "query_index":
            index = arguments.get("index")
            filters = arguments.get("filters", {})
            size = arguments.get("size", 100)
            from_ = arguments.get("from_", 0)
            sort = arguments.get("sort")
            
            result = await es_manager.query_index(
                index, filters, size, from_, sort, INDEX_CONFIG
            )
            
            # Обработка данных: дедупликация + алиасы
            result = process_elasticsearch_data(result, index, INDEX_CONFIG)
            
            logger.info(f"Query result size: {len(str(result))} characters")
            if isinstance(result, dict) and 'hits' in result:
                logger.info(f"Hits count: {len(result['hits']['hits'])}")
            
            # Добавляем информацию о ретенции в начало ответа
            retention_info = get_data_retention_info()
            result_text = f"{retention_info}\n\n" + json.dumps(result, ensure_ascii=False, indent=2, default=str)
            return [TextContent(type="text", text=result_text)]
            
        elif name == "create_plot":
            if not plot_manager.is_available():
                return [TextContent(type="text", text="Ошибка: matplotlib/pandas не установлены")]
            
            index = arguments.get("index")
            filters = arguments.get("filters", {})
            plot_type = arguments.get("plot_type")
            x_field = arguments.get("x_field")
            y_field = arguments.get("y_field")
            group_by = arguments.get("group_by")
            title = arguments.get("title", "График")
            size = arguments.get("size", 100)
            
            # Получаем данные из Elasticsearch
            result = await es_manager.query_index(
                index, filters, size, 0, [{"@timestamp": {"order": "asc"}}], INDEX_CONFIG
            )
            
            # Обработка данных: дедупликация + алиасы
            result = process_elasticsearch_data(result, index, INDEX_CONFIG)
            
            # Создаем график
            plot_result = await plot_manager.create_plot_from_data(
                result, plot_type, x_field, y_field, group_by, title
            )
            
            # Добавляем информацию о ретенции в начало ответа
            retention_info = get_data_retention_info()
            final_result = f"{retention_info}\n\n{plot_result}"
            return [TextContent(type="text", text=final_result)]
            
        else:
            return [TextContent(type="text", text=f"Неизвестный инструмент: {name}")]
            
    except Exception as e:
        logger.error(f"Ошибка выполнения инструмента {name}: {e}")
        return [TextContent(type="text", text=f"Ошибка: {str(e)}")]

def show_help():
    """Показывает справку по использованию"""
    print("MCP сервер для работы с Elasticsearch (APM) - РЕФАКТОРЕННАЯ ВЕРСИЯ\n")
    print("Использование:")
    print("  python mcp_apm_server_refactored.py     - Запуск MCP сервера")
    print("  python mcp_apm_server_refactored.py --help - Показать справку\n")
    print("Доступные инструменты MCP:")
    print("  • list_indexes   - Список индексов и их описание")
    print("  • get_data_retention_info - Получить информацию о доступном периоде данных")
    print("  • query_index    - Выполнить запрос к индексу Elasticsearch")
    if plot_manager.is_available():
        print("  • create_plot    - Создать график по данным из Elasticsearch")
    else:
        print("  • create_plot    - НЕДОСТУПНО (нет matplotlib/pandas)")
    
    print("\nИнформация о данных:")
    retention_info = get_data_retention_info()
    print(f"  {retention_info}")
    
    print("\nСтруктура модулей:")
    print("  • src/config_utils.py      - утилиты конфигурации")  
    print("  • src/elasticsearch_client.py - клиент Elasticsearch")
    print("  • src/data_processing.py   - обработка данных")
    print("  • src/plotting.py          - создание графиков")

async def main():
    """Главная функция запуска сервера"""
    import sys
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ['--help', '-h']:
            show_help()
            return
        else:
            print(f"Неизвестный аргумент: {arg}")
            print("Используйте --help для справки")
            return
    
    logger.info("Запуск рефакторенного MCP сервера для работы с Elasticsearch (APM)")
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main()) 
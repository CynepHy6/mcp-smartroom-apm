#!/usr/bin/env python3
"""
MCP сервер для безопасной работы с Elasticsearch (APM)
Работает через MCP Server, поддерживает инструменты для работы с индексами и запросами
"""

import os
import logging
import json
import yaml
import asyncio
from typing import Dict, Any, List, Optional, Sequence
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch, exceptions as es_exceptions
from mcp.server import Server
from mcp.types import Tool, TextContent, LoggingLevel
import mcp.server.stdio
from mcp.server.lowlevel import Server as LowLevelServer
from mcp.server.models import InitializationOptions
from urllib.parse import urlparse
from datetime import datetime
import base64
import io
import hashlib
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')  # Используем backend без GUI
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import pandas as pd
    import numpy as np
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp-apm-server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if not PLOTTING_AVAILABLE:
    logger.warning("Matplotlib/pandas не установлены. Функция построения графиков недоступна.")

load_dotenv()
INDEX_CONFIG_PATH = "index.yaml"
if not os.path.exists(INDEX_CONFIG_PATH):
    logger.error(f"Файл {INDEX_CONFIG_PATH} не найден")
    raise FileNotFoundError(f"File {INDEX_CONFIG_PATH} not found")

with open(INDEX_CONFIG_PATH, encoding="utf-8") as f:
    raw_config = yaml.safe_load(f) or {}

INDEX_CONFIG = {}
for index_name, content in raw_config.items():
    if not isinstance(content, dict):
        logger.warning(f"Некорректная структура для индекса '{index_name}' в index.yaml")
        continue
    fields = {}
    events = []
    if "events" in content and isinstance(content["events"], list):
        for item in content["events"]:
            if isinstance(item, dict):
                for key, value in item.items():
                    events.append({"name": key, "description": value})
            elif isinstance(item, str):
                events.append({"name": item, "description": ""})
    if "fields" in content and isinstance(content["fields"], dict):
        fields = content["fields"]
    else:
        fields = {k: v for k, v in content.items() if k != "events"}
    INDEX_CONFIG[index_name] = {
        "fields": fields,
        "events": events
    }
class ElasticsearchManager:
    def __init__(self):
        self.client = AsyncElasticsearch(
            hosts=[os.getenv("APM_BASE_URL")],
            basic_auth=(os.getenv("APM_USERNAME"), os.getenv("APM_PASSWORD")),
            request_timeout=int(os.getenv("APM_TIMEOUT", "30"))
        )

    async def list_indexes(self) -> List[Dict[str, Any]]:
        """Список индексов и их описание"""
        result = []
        for idx, data in INDEX_CONFIG.items():
            result.append({
                "name": idx,
                "fields": data["fields"],
                "events": data["events"]
            })
        return result

    async def query_index(self, index: str, filters: Dict[str, Any], size: int = 100, from_: int = 0, sort: Optional[List[Dict]] = None) -> Dict[str, Any]:
        if index not in INDEX_CONFIG:
            raise ValueError(f"Индекс '{index}' не найден в index.yaml")
        
        if "bool" in filters or "match" in filters or "term" in filters or "range" in filters:
            query_body = {
                "query": filters,
                "size": size,
                "from": from_
            }
        else:
            query_body = {
                "query": {
                    "bool": {
                        "must": []
                    }
                },
                "size": size,
                "from": from_
            }
            for key, value in filters.items():
                if isinstance(value, dict) and ("gte" in value or "lte" in value):
                    query_body["query"]["bool"]["must"].append({
                        "range": {key: value}
                    })
                else:
                    query_body["query"]["bool"]["must"].append({
                        "term": {key: value}
                    })
        
        if sort:
            query_body["sort"] = sort
            
        logger.info(f"Elasticsearch query to index '{index}': {query_body}")
        try:
            result = await self.client.search(index=index, body=query_body)
            return result.body
        except es_exceptions.AuthenticationException:
            raise RuntimeError("Ошибка аутентификации Elasticsearch")
        except es_exceptions.ConnectionError:
            raise RuntimeError("Ошибка подключения к Elasticsearch")
        except Exception as e:
            raise RuntimeError(f"Ошибка Elasticsearch: {e}")

es_manager = ElasticsearchManager()
server = Server("apm-server")

@server.list_tools()
async def list_tools() -> list[Tool]:
    tools = [
        Tool(
            name="list_indexes",
            description="Получить список доступных индексов и их описание",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="query_index",
            description="Выполнить запрос к индексу Elasticsearch",
            inputSchema={
                "type": "object",
                "properties": {
                    "index": {"type": "string", "description": "Имя индекса"},
                    "filters": {"type": "object", "description": "Фильтры запроса (term/range)"},
                    "size": {"type": "integer", "description": "Размер выборки", "default": 100},
                    "from_": {"type": "integer", "description": "Смещение", "default": 0},
                    "sort": {"type": "array", "items": {"type": "object"}, "description": "Сортировка"}
                },
                "required": ["index", "filters"]
            }
        )
    ]
    
    if PLOTTING_AVAILABLE:
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
    try:
        if name == "list_indexes":
            result = await es_manager.list_indexes()
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
        elif name == "query_index":
            index = arguments.get("index")
            filters = arguments.get("filters", {})
            size = arguments.get("size", 100)
            from_ = arguments.get("from_", 0)
            sort = arguments.get("sort")
            result = await es_manager.query_index(index, filters, size, from_, sort)
            logger.info(f"Query result size: {len(str(result))} characters")
            if isinstance(result, dict) and 'hits' in result:
                logger.info(f"Hits count: {len(result['hits']['hits'])}")
            
            allowed_fields = get_configured_fields(index)
            
            if allowed_fields and isinstance(result, dict) and 'hits' in result and 'hits' in result['hits']:
                logger.info(f"Filtering fields according to config. Allowed fields: {allowed_fields}")
                original_size = len(str(result))
                for hit in result['hits']['hits']:
                    if '_source' in hit:
                        original_source_size = len(str(hit['_source']))
                        hit['_source'] = filter_source_fields(hit['_source'], allowed_fields)
                        filtered_source_size = len(str(hit['_source']))
                        logger.info(f"Filtered source: {original_source_size} -> {filtered_source_size} chars")
                new_size = len(str(result))
                logger.info(f"Total filtering: {original_size} -> {new_size} chars")
            else:
                logger.info(f"Skipping filtering: allowed_fields={bool(allowed_fields)}")
            
            result_text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
            logger.info(f"JSON serialization successful, length: {len(result_text)}")
            return [TextContent(
                type="text",
                text=result_text
            )]
        elif name == "create_plot":
            if not PLOTTING_AVAILABLE:
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
            result = await es_manager.query_index(index, filters, size, 0, [{"@timestamp": {"order": "asc"}}])
            
            # Создаем график
            plot_result = await create_plot_from_data(
                result, plot_type, x_field, y_field, group_by, title
            )
            
            return [TextContent(type="text", text=plot_result)]
        else:
            return [TextContent(type="text", text=f"Неизвестный инструмент: {name}")]
    except Exception as e:
        logger.error(f"Ошибка выполнения инструмента {name}: {e}")
        return [TextContent(type="text", text=f"Ошибка: {str(e)}")]

def show_help():
    print("MCP сервер для работы с Elasticsearch (APM)\n")
    print("Использование:")
    print("  mcp-apm-mcp-server                 - Запуск MCP сервера")
    print("  mcp-apm-mcp-server --help           - Показать эту справку\n")
    print("Доступные инструменты MCP:")
    print("  • list_indexes   - Список индексов и их описание")
    print("  • query_index    - Выполнить запрос к индексу Elasticsearch")
    if PLOTTING_AVAILABLE:
        print("  • create_plot    - Создать график по данным из Elasticsearch")
    else:
        print("  • create_plot    - НЕДОСТУПНО (нет matplotlib/pandas)")
    print("\nКонфигурация:")
    print("  index.yaml - описание индексов")
    print("  .env       - креды для Elasticsearch")

def get_configured_fields(index_name: str) -> List[str]:
    """Получает список полей для индекса из конфигурации"""
    if index_name not in INDEX_CONFIG:
        return []
    
    config = INDEX_CONFIG[index_name]
    if "fields" in config and isinstance(config["fields"], dict):
        return list(config["fields"].keys())
    return []

def filter_source_fields(source: Dict[str, Any], allowed_fields: List[str]) -> Dict[str, Any]:
    """Фильтрует поля _source согласно конфигурации
    
    Поддерживает как плоские поля, так и вложенные объекты с точечной нотацией.
    """
    filtered = {}
    
    def get_nested_value(obj: Dict[str, Any], path: str) -> Any:
        """Получает значение по пути с точками (например, 'details.summary.mos')"""
        keys = path.split('.')
        current = obj
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def set_nested_value(obj: Dict[str, Any], path: str, value: Any) -> None:
        """Устанавливает значение по пути с точками"""
        keys = path.split('.')
        current = obj
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value
    
    for field in allowed_fields:
        # Сначала проверяем плоское поле
        if field in source:
            filtered[field] = source[field]
        else:
            # Затем проверяем вложенное поле
            value = get_nested_value(source, field)
            if value is not None:
                set_nested_value(filtered, field, value)
    
    return filtered

def get_nested_value(obj: Dict[str, Any], path: str) -> Any:
    """Получает значение по пути с точками"""
    keys = path.split('.')
    current = obj
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current

async def create_plot_from_data(es_result: Dict[str, Any], plot_type: str, x_field: str, y_field: str, group_by: Optional[str], title: str) -> str:
    """Создает график по данным из Elasticsearch и сохраняет в файл"""
    if not PLOTTING_AVAILABLE:
        return "Ошибка: matplotlib/pandas не установлены"
    
    try:
        # Извлекаем данные
        records = []
        for hit in es_result.get('hits', {}).get('hits', []):
            source = hit['_source']
            
            # Получаем значения полей
            x_value = get_nested_value(source, x_field) if '.' in x_field else source.get(x_field)
            y_value = get_nested_value(source, y_field) if '.' in y_field else source.get(y_field)
            group_value = get_nested_value(source, group_by) if group_by and '.' in group_by else source.get(group_by) if group_by else None
            
            if x_value is not None and y_value is not None:
                record = {
                    'x': x_value,
                    'y': y_value,
                    'group': group_value
                }
                records.append(record)
        
        if not records:
            return "Нет данных для построения графика"
        
        # Создаем DataFrame
        df = pd.DataFrame(records)
        
        # Обрабатываем временные данные
        if x_field == '@timestamp' or 'timestamp' in x_field.lower():
            df['x'] = pd.to_datetime(df['x'])
        
        # Настройка графика
        plt.figure(figsize=(12, 8))
        
        if plot_type == "mos_timeline":
            # Специальный график для МОС
            if group_by:
                groups = df['group'].unique()
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
                
                for i, group in enumerate(groups):
                    group_data = df[df['group'] == group].sort_values('x')
                    plt.plot(group_data['x'], group_data['y'], 
                            marker='o', markersize=4, linewidth=2,
                            color=colors[i % len(colors)],
                            label=f'{group_by}: {group}')
                
                plt.legend()
            else:
                plt.plot(df['x'], df['y'], marker='o', markersize=4, linewidth=2)
            
            plt.ylim(3.5, 5.0)
            plt.axhline(y=4.0, color='red', linestyle='--', alpha=0.5, label='Хорошее качество (4.0)')
            plt.axhline(y=4.5, color='green', linestyle='--', alpha=0.5, label='Отличное качество (4.5)')
            
        elif plot_type == "line":
            if group_by:
                groups = df['group'].unique()
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
                
                for i, group in enumerate(groups):
                    group_data = df[df['group'] == group].sort_values('x')
                    plt.plot(group_data['x'], group_data['y'], 
                            marker='o', markersize=3, linewidth=1.5,
                            color=colors[i % len(colors)],
                            label=f'{group_by}: {group}')
                
                plt.legend()
            else:
                plt.plot(df['x'], df['y'], marker='o', markersize=3, linewidth=1.5)
                
        elif plot_type == "scatter":
            if group_by:
                groups = df['group'].unique()
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
                
                for i, group in enumerate(groups):
                    group_data = df[df['group'] == group]
                    plt.scatter(group_data['x'], group_data['y'], 
                              color=colors[i % len(colors)],
                              label=f'{group_by}: {group}', alpha=0.7)
                
                plt.legend()
            else:
                plt.scatter(df['x'], df['y'], alpha=0.7)
                
        elif plot_type == "bar":
            if group_by:
                grouped = df.groupby('group')['y'].mean()
                plt.bar(range(len(grouped)), grouped.values)
                plt.xticks(range(len(grouped)), grouped.index, rotation=45)
            else:
                # Группируем по X и берем среднее Y
                grouped = df.groupby('x')['y'].mean()
                plt.bar(range(len(grouped)), grouped.values)
                plt.xticks(range(len(grouped)), grouped.index, rotation=45)
        
        # Настройка осей и заголовков
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(x_field, fontsize=12)
        plt.ylabel(y_field, fontsize=12)
        
        # Форматирование временной оси
        if x_field == '@timestamp' or 'timestamp' in x_field.lower():
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            plt.xticks(rotation=45)
        
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Создаем уникальное имя файла
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        
        # Создаем хеш от параметров для уникальности
        params_str = f"{plot_type}_{x_field}_{y_field}_{group_by}_{title}_{len(df)}"
        file_hash = hashlib.md5(params_str.encode()).hexdigest()[:8]
        
        # Создаем понятное имя файла
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_title = safe_title.replace(' ', '_')[:30]  # Ограничиваем длину
        
        filename = f"{now.strftime('%Y%m%d_%H%M%S')}_{safe_title}_{plot_type}_{file_hash}.png"
        
        # Создаем структуру папок
        plots_dir = Path("plots") / year / month
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = plots_dir / filename
        
        # Сохраняем график в файл
        plt.savefig(file_path, format='png', dpi=150, bbox_inches='tight')
        
        # Также сохраняем в base64 для обратной совместимости
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
        buffer.seek(0)
        plot_base64 = base64.b64encode(buffer.getvalue()).decode()
        plt.close()
        
        # Статистика
        stats = {
            "total_points": len(df),
            "x_range": [str(df['x'].min()), str(df['x'].max())],
            "y_range": [float(df['y'].min()), float(df['y'].max())],
            "y_mean": float(df['y'].mean()),
            "y_std": float(df['y'].std())
        }
        
        if group_by:
            stats["groups"] = df['group'].unique().tolist()
        
        result = {
            "status": "success",
            "file_path": str(file_path.absolute()),
            "file_size": file_path.stat().st_size,
            "plot_base64": plot_base64,
            "statistics": stats,
            "message": f"График создан успешно и сохранен в {file_path.absolute()}. Тип: {plot_type}, точек данных: {len(df)}"
        }
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        logger.error(f"Ошибка создания графика: {e}")
        return f"Ошибка создания графика: {str(e)}"

async def main():
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
    logger.info("Запуск MCP сервера для работы с Elasticsearch (APM)")
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main()) 
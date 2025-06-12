#!/usr/bin/env python3
"""
Утилиты для обработки данных из Elasticsearch
"""

import logging
from typing import Dict, Any, List, Union

logger = logging.getLogger(__name__)

def get_nested_value(obj: Dict[str, Any], path: str) -> Any:
    """
    Получает значение по пути с точками, поддерживает массивы
    Поддерживает синтаксис:
    - "field.subfield" - обычный путь
    - "field[].subfield" - извлечение subfield из всех элементов массива field
    - "field[0].subfield" - извлечение subfield из конкретного элемента массива
    """
    return _traverse_path(obj, path)

def _traverse_path(obj: Any, path: str) -> Any:
    """Рекурсивно обходит путь, обрабатывая массивы"""
    if not path:
        return obj
    
    # Разбираем первую часть пути
    if '.' in path:
        first_part, rest_path = path.split('.', 1)
    else:
        first_part, rest_path = path, ''
    
    # Обрабатываем массивы
    if first_part.endswith('[]'):
        # Извлечение из всех элементов массива
        field_name = first_part[:-2]  # убираем []
        if not isinstance(obj, dict) or field_name not in obj:
            return None
        
        array_field = obj[field_name]
        if not isinstance(array_field, list):
            return None
        
        # Собираем значения из всех элементов массива
        results = []
        for item in array_field:
            if rest_path:
                value = _traverse_path(item, rest_path)
            else:
                value = item
            
            if value is not None:
                if isinstance(value, list):
                    results.extend(value)
                else:
                    results.append(value)
        
        return results if results else None
    
    elif '[' in first_part and first_part.endswith(']'):
        # Извлечение из конкретного элемента массива
        field_name, index_part = first_part.split('[', 1)
        index_str = index_part[:-1]  # убираем ]
        
        try:
            index = int(index_str)
        except ValueError:
            return None
        
        if not isinstance(obj, dict) or field_name not in obj:
            return None
        
        array_field = obj[field_name]
        if not isinstance(array_field, list) or index >= len(array_field):
            return None
        
        target_item = array_field[index]
        return _traverse_path(target_item, rest_path) if rest_path else target_item
    
    else:
        # Обычное поле
        if not isinstance(obj, dict) or first_part not in obj:
            return None
        
        next_obj = obj[first_part]
        return _traverse_path(next_obj, rest_path) if rest_path else next_obj

def format_extracted_values(values: Any) -> str:
    """Форматирует извлеченные значения в строку"""
    if values is None:
        return None
    
    if isinstance(values, list):
        # Преобразуем все в строки и убираем дубликаты
        str_values = [str(v) for v in values if v is not None]
        unique_values = list(dict.fromkeys(str_values))  # сохраняем порядок
        return ', '.join(unique_values) if unique_values else None
    
    return str(values)

def set_nested_value(obj: Dict[str, Any], path: str, value: Any) -> None:
    """Устанавливает значение по пути с точками"""
    keys = path.split('.')
    current = obj
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value

def deduplicate_field_values(value: str) -> str:
    """Дедуплицирует значения в поле, разделенные запятыми"""
    if not value or not isinstance(value, str):
        return value
    
    items = [item.strip() for item in value.split(',')]
    unique_items = list(dict.fromkeys(items))
    return ', '.join(unique_items)

def apply_field_aliases(source: Dict[str, Any], fields_config: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Применяет алиасы к полям в _source, создает чистый объект только с нужными полями"""
    new_source = {}  # Создаем пустой объект
    
    # Список важных полей, которые всегда сохраняем
    important_fields = ['@timestamp', 'userId', 'userRole', 'event', 'appSessionId']
    
    # Сначала копируем важные поля
    for field in important_fields:
        if field in source:
            new_source[field] = source[field]
    
    # Затем обрабатываем поля из конфигурации
    for field_name, field_config in fields_config.items():
        # Универсальное извлечение значения (поддерживает массивы)
        raw_value = get_nested_value(source, field_name)
        
        if raw_value is not None:
            # Форматируем значение в строку только если это массив
            if isinstance(raw_value, list):
                value = format_extracted_values(raw_value)
            else:
                value = raw_value
            
            if value is not None:
                alias = field_config.get("alias")
                if alias:
                    # Дедупликация если нужна (только для строк)
                    if field_config.get("need_dedupe") and isinstance(value, str):
                        value = deduplicate_field_values(value)
                    
                    # Устанавливаем алиас
                    if '.' in alias:
                        set_nested_value(new_source, alias, value)
                    else:
                        new_source[alias] = value
                else:
                    # Если алиаса нет, но поле в конфигурации - сохраняем исходное поле
                    if '.' in field_name:
                        set_nested_value(new_source, field_name, value)
                    else:
                        new_source[field_name] = value
    
    return new_source

def process_elasticsearch_data(result: Dict[str, Any], index_name: str, index_config: Dict[str, Any]) -> Dict[str, Any]:
    """Обрабатывает данные из Elasticsearch: дедупликация + алиасы"""
    if not isinstance(result, dict) or 'hits' not in result:
        return result
    
    config = index_config.get(index_name, {})
    fields_config = config.get("fields", {})
    
    for hit in result['hits']['hits']:
        if '_source' not in hit:
            continue
            
        source = hit['_source']
        
        # Применение алиасов (включает дедупликацию)
        source = apply_field_aliases(source, fields_config)
        hit['_source'] = source
    
    return result 
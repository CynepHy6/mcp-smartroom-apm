#!/usr/bin/env python3
"""
Утилиты для обработки данных из Elasticsearch
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

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
    """Применяет алиасы к полям в _source"""
    new_source = {}
    
    for field_name, field_config in fields_config.items():
        value = get_nested_value(source, field_name)
        if value is not None:
            alias = field_config.get("alias")
            final_field_name = alias if alias else field_name
            
            if '.' in final_field_name:
                set_nested_value(new_source, final_field_name, value)
            else:
                new_source[final_field_name] = value
    
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
        
        # 1. Дедупликация полей
        for field_name, field_config in fields_config.items():
            if field_config.get("need_dedupe"):
                value = get_nested_value(source, field_name)
                if value and isinstance(value, str):
                    deduplicated = deduplicate_field_values(value)
                    set_nested_value(source, field_name, deduplicated)
        
        # 2. Применение алиасов
        source = apply_field_aliases(source, fields_config)
        hit['_source'] = source
    
    return result 
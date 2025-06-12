#!/usr/bin/env python3
"""
Утилиты для работы с конфигурацией индексов
"""

import os
import yaml
import logging
from typing import Dict, Any, Union
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def get_retention_dates_info():
    """Получить динамическую информацию о доступных датах"""
    now = datetime.now()
    retention_days = 20
    oldest_available = now - timedelta(days=retention_days)
    return f"ВАЖНО: Логи хранятся не более {retention_days} дней. Поиск данных старше этого периода может не дать результатов.  (сейчас {now.strftime('%Y-%m-%d %H:%M:%S')})"

def parse_field_config(config: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Парсит конфигурацию поля: поддерживает простой и расширенный формат"""
    if isinstance(config, str):
        return {
            "description": config,
            "alias": None,
            "need_dedupe": False
        }
    elif isinstance(config, dict):
        return {
            "description": config.get("description", ""),
            "alias": config.get("alias"),
            "need_dedupe": config.get("need_dedupe", False)
        }
    else:
        logger.warning(f"Некорректный формат конфигурации поля: {config}")
        return {
            "description": str(config),
            "alias": None,
            "need_dedupe": False
        }

def validate_index_config(index_config: Dict[str, Any]) -> None:
    """Валидирует конфигурацию индексов при запуске"""
    logger.info("Валидация конфигурации индексов...")
    
    for index_name, config in index_config.items():
        fields = config.get("fields", {})
        
        # Проверяем алиасы на уникальность
        aliases = {}
        for field_name, field_config in fields.items():
            alias = field_config.get("alias")
            if alias:
                if alias in aliases:
                    logger.error(f"Дублированный алиас '{alias}' в индексе '{index_name}': поля '{aliases[alias]}' и '{field_name}'")
                else:
                    aliases[alias] = field_name
        
        # Проверяем корректность полей с дедупликацией
        dedupe_fields = [name for name, cfg in fields.items() if cfg.get("need_dedupe")]
        if dedupe_fields:
            logger.info(f"Индекс '{index_name}': поля с дедупликацией: {dedupe_fields}")
    
    logger.info("Валидация конфигурации завершена")

def load_index_config(config_path: str = "index.yaml") -> Dict[str, Any]:
    """Загружает и парсит конфигурацию индексов"""
    if not os.path.exists(config_path):
        logger.error(f"Файл {config_path} не найден")
        raise FileNotFoundError(f"File {config_path} not found")

    with open(config_path, encoding="utf-8") as f:
        raw_config = yaml.safe_load(f) or {}

    index_config = {}
    for index_name, content in raw_config.items():
        if not isinstance(content, dict):
            logger.warning(f"Некорректная структура для индекса '{index_name}' в {config_path}")
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
        
        # Парсим расширенный формат полей
        parsed_fields = {}
        for field_name, field_config in fields.items():
            parsed_config = parse_field_config(field_config)
            
            # Если поле помечено как need_now, обновляем описание с текущими датами
            if parsed_config.get("need_now"):
                parsed_config["description"] = get_retention_dates_info()
            
            parsed_fields[field_name] = parsed_config
        
        index_config[index_name] = {
            "fields": parsed_fields,
            "events": events
        }
    
    validate_index_config(index_config)
    return index_config 
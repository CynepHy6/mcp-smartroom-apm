#!/usr/bin/env python3
"""
Тест для проверки работы list_indexes
"""

import pytest
from datetime import datetime
from src.config_utils import load_index_config
from src.elasticsearch_client import ElasticsearchManager

@pytest.mark.asyncio
async def test_list_indexes():
    """Тест функции list_indexes"""
    
    # Загружаем конфигурацию через load_index_config (обработанную)
    index_config = load_index_config()
    
    # Создаем экземпляр ElasticsearchManager
    es_manager = ElasticsearchManager()
    
    try:
        # Вызываем list_indexes
        indexes = await es_manager.list_indexes(index_config)
        
        # Проверяем, что результат не пустой
        assert len(indexes) > 0, "Должен быть хотя бы один индекс"
        
        # Находим индекс logs_videocall
        logs_videocall = None
        for index_info in indexes:
            if index_info['name'] == 'logs_videocall':
                logs_videocall = index_info
                break
        
        assert logs_videocall is not None, "Индекс logs_videocall должен существовать"
        
        # Проверяем, что поле @timestamp существует
        timestamp_field = logs_videocall['fields'].get('@timestamp')
        assert timestamp_field is not None, "Поле @timestamp должно существовать"
        
        description = timestamp_field['description']
        assert 'ВАЖНО' in description, "В описании поля @timestamp должна быть важная информация"
        assert 'Логи хранятся не более 20 дней' in description, "В описании должна быть информация о ретенции"
        
        print(f"✅ Тест прошел успешно. Описание @timestamp: {description}")
        
    finally:
        # Закрываем соединение с Elasticsearch
        await es_manager.client.close() 
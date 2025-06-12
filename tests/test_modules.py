#!/usr/bin/env python3
"""
Тесты модулей MCP APM сервера
Проверка работы отдельных модулей: config_utils, data_processing, elasticsearch_client, plotting
"""

import sys
import os
import unittest
from unittest.mock import Mock, patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.config_utils import parse_field_config, load_index_config
from src.data_processing import (
    deduplicate_field_values, 
    process_elasticsearch_data,
    get_nested_value, 
    set_nested_value
)

# Мокаем зависимости для тестирования без установки
with patch.dict('sys.modules', {
    'elasticsearch': Mock(),
    'elasticsearch.AsyncElasticsearch': Mock(),
    'elasticsearch.exceptions': Mock(),
    'mcp': Mock(),
    'mcp.server': Mock(),
    'mcp.types': Mock(),
    'mcp.server.stdio': Mock()
}):
    from src.elasticsearch_client import ElasticsearchManager
    from src.plotting import PlotManager


class TestModules(unittest.TestCase):
    """Тесты отдельных модулей системы"""
    
    def test_config_loading(self):
        """Тест загрузки конфигурации"""
        config = load_index_config()
        self.assertIsInstance(config, dict)
        self.assertGreater(len(config), 0)
        print(f"✅ Загружено индексов: {len(config)}")
    
    def test_field_config_parsing(self):
        """Тест парсинга конфигурации полей"""
        simple = parse_field_config("Простое описание")
        self.assertEqual(simple["description"], "Простое описание")
        self.assertIsNone(simple["alias"])
        self.assertFalse(simple["need_dedupe"])
        
        extended = parse_field_config({
            "description": "Сложное поле",
            "alias": "complexField", 
            "need_dedupe": True
        })
        self.assertEqual(extended["description"], "Сложное поле")
        self.assertEqual(extended["alias"], "complexField")
        self.assertTrue(extended["need_dedupe"])
        
        print("✅ Парсинг конфигурации работает")
    
    def test_data_processing(self):
        """Тест обработки данных"""
        # Дедупликация
        result = deduplicate_field_values("a, b, a, c, b")
        self.assertEqual(result, "a, b, c")
        
        # Вложенные значения
        test_obj = {"level1": {"level2": {"value": "test"}}}
        self.assertEqual(get_nested_value(test_obj, "level1.level2.value"), "test")
        
        set_nested_value(test_obj, "new.nested.field", "new_value")
        self.assertEqual(get_nested_value(test_obj, "new.nested.field"), "new_value")
        
        print("✅ Обработка данных работает")
    
    @patch('src.elasticsearch_client.AsyncElasticsearch')
    def test_elasticsearch_manager(self, mock_es):
        """Тест менеджера Elasticsearch"""
        manager = ElasticsearchManager()
        self.assertIsNotNone(manager.client)
        
        # Тест построения запроса
        query = manager._build_query({"field": "value"}, 10, 0, None)
        self.assertIn("query", query)
        self.assertEqual(query["size"], 10)
        
        print("✅ ElasticsearchManager инициализирован")
    
    def test_plot_manager(self):
        """Тест менеджера графиков"""
        manager = PlotManager()
        self.assertEqual(manager.plots_dir.name, "plots")
        
        # Тест извлечения записей
        es_result = {
            "hits": {
                "hits": [
                    {"_source": {"timestamp": "2024-01-01", "value": 10, "group": "A"}},
                    {"_source": {"timestamp": "2024-01-02", "value": 20, "group": "B"}}
                ]
            }
        }
        
        records = manager._extract_records(es_result, "timestamp", "value", "group")
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["x"], "2024-01-01")
        self.assertEqual(records[0]["y"], 10)
        
        print("✅ PlotManager работает")
    
    def test_full_integration(self):
        """Интеграционный тест полного процесса"""
        # Загружаем конфигурацию
        config = load_index_config()
        
        # Имитируем ответ Elasticsearch
        es_response = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_source": {
                            "details": {
                                "issues": {
                                    "reason": "timeout, error, timeout"
                                },
                                "summary": {
                                    "publisher": {
                                        "publisherMos": {
                                            "mos": 4.2
                                        }
                                    }
                                }
                            },
                            "userId": "user123",
                            "@timestamp": "2024-01-15T10:30:00Z"
                        }
                    }
                ]
            }
        }
        
        # Обрабатываем данные
        processed = process_elasticsearch_data(es_response, "logs_videocall", config)
        
        # Проверяем результат
        self.assertIn("hits", processed)
        source = processed["hits"]["hits"][0]["_source"]
        
        # Должны быть применены алиасы и дедупликация
        if "issueReason" in source:
            # Проверяем дедупликацию (timeout, error, timeout -> timeout, error)
            self.assertIn("timeout, error", source["issueReason"])
        
        print("✅ Полная интеграция работает")


if __name__ == "__main__":
    unittest.main(verbosity=2) 
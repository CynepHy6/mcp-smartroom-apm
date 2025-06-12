#!/usr/bin/env python3
"""
Интеграционные тесты MCP APM сервера
Проверка полного цикла работы: конфигурация, обработка данных, производительность
"""

import pytest
import sys
import json
import os
import unittest
from unittest.mock import Mock, patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.config_utils import parse_field_config, load_index_config
from src.data_processing import (
    deduplicate_field_values,
    apply_field_aliases,
    process_elasticsearch_data,
    get_nested_value,
    set_nested_value
)


class TestIntegration(unittest.TestCase):
    """Интеграционные тесты полного функционала"""
    
    def setUp(self):
        """Настройка для каждого теста"""
        self.index_config = load_index_config()
    
    def test_parse_field_config_simple(self):
        """Тест парсинга простого формата конфигурации поля"""
        simple_config = "Описание поля"
        result = parse_field_config(simple_config)
        
        expected = {
            "description": "Описание поля",
            "alias": None,
            "need_dedupe": False
        }
        
        self.assertEqual(result, expected)
        print("✅ Парсинг простого формата работает")
    
    def test_parse_field_config_extended(self):
        """Тест парсинга расширенного формата конфигурации поля"""
        extended_config = {
            "description": "Причина проблемы",
            "alias": "issueReason",
            "need_dedupe": True
        }
        result = parse_field_config(extended_config)
        
        expected = {
            "description": "Причина проблемы",
            "alias": "issueReason",
            "need_dedupe": True
        }
        
        self.assertEqual(result, expected)
        print("✅ Парсинг расширенного формата работает")
    
    def test_deduplicate_field_values(self):
        """Тест дедупликации значений в полях"""
        test_cases = [
            ("error, warning, error, info", "error, warning, info"),
            ("timeout, timeout, connection", "timeout, connection"),
            ("single", "single"),
            ("", ""),
            (None, None),
            ("a,b,c,a,b", "a, b, c")
        ]
        
        for input_val, expected in test_cases:
            result = deduplicate_field_values(input_val)
            self.assertEqual(result, expected, f"Ошибка: {input_val} -> {result}, ожидалось {expected}")
        
        print("✅ Дедупликация работает корректно")
    
    def test_nested_values(self):
        """Тест работы с вложенными значениями"""
        test_data = {
            "details": {
                "issues": {
                    "reason": "timeout, error, timeout"
                },
                "summary": {
                    "publisher": {
                        "publisherMos": {
                            "mos": 4.2,
                            "avgJitter": 15.5
                        }
                    }
                }
            },
            "userId": "user123"
        }
        
        # Тест получения значений
        self.assertEqual(get_nested_value(test_data, "details.issues.reason"), "timeout, error, timeout")
        self.assertEqual(get_nested_value(test_data, "details.summary.publisher.publisherMos.mos"), 4.2)
        self.assertEqual(get_nested_value(test_data, "userId"), "user123")
        self.assertIsNone(get_nested_value(test_data, "non.existent.field"))
        
        # Тест установки значений
        set_nested_value(test_data, "new.nested.field", "test_value")
        self.assertEqual(get_nested_value(test_data, "new.nested.field"), "test_value")
        
        print("✅ Работа с вложенными значениями корректна")
    
    def test_apply_field_aliases(self):
        """Тест применения алиасов к полям"""
        source_data = {
            "details": {
                "issues": {
                    "reason": "timeout, error, timeout"
                },
                "summary": {
                    "publisher": {
                        "publisherMos": {
                            "mos": 4.2,
                            "avgJitter": 15.5,
                            "rtt": 25.1
                        }
                    }
                }
            },
            "userId": "user123",
            "userRole": "student"
        }
        
        fields_config = {
            "details.issues.reason": {
                "description": "Причина проблемы",
                "alias": "issueReason",
                "need_dedupe": True
            },
            "details.summary.publisher.publisherMos.mos": {
                "description": "МОС",
                "alias": "mos",
                "need_dedupe": False
            },
            "details.summary.publisher.publisherMos.avgJitter": {
                "description": "Среднее значение задержки",
                "alias": "avgJitter",
                "need_dedupe": False
            },
            "userId": {
                "description": "ID пользователя",
                "alias": None,
                "need_dedupe": False
            }
        }
        
        result = apply_field_aliases(source_data, fields_config)
        
        # Проверяем что алиасы применились
        self.assertIn("issueReason", result)
        self.assertIn("mos", result)
        self.assertIn("avgJitter", result)
        self.assertIn("userId", result)  # Без алиаса остается как есть
        
        # Проверяем что значения корректны
        self.assertEqual(result["issueReason"], "timeout, error, timeout")
        self.assertEqual(result["mos"], 4.2)
        self.assertEqual(result["avgJitter"], 15.5)
        self.assertEqual(result["userId"], "user123")
        
        # Проверяем что старые поля исчезли
        self.assertEqual(len([k for k in result.keys() if "details." in k]), 0)
        
        print("✅ Применение алиасов работает корректно")
    
    def test_full_processing_pipeline(self):
        """Тест полного цикла обработки данных Elasticsearch"""
        # Имитируем ответ Elasticsearch
        es_response = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {
                        "_source": {
                            "details": {
                                "issues": {
                                    "reason": "timeout, connection, timeout, error"
                                },
                                "summary": {
                                    "publisher": {
                                        "publisherMos": {
                                            "mos": 3.8,
                                            "avgJitter": 28.3,
                                            "rtt": 45.2
                                        }
                                    }
                                }
                            },
                            "userId": "user456",
                            "userRole": "student",
                            "event": "webrtcIssue",
                            "@timestamp": "2024-01-15T10:30:00Z"
                        }
                    },
                    {
                        "_source": {
                            "details": {
                                "issues": {
                                    "reason": "audio, video, audio"
                                },
                                "summary": {
                                    "publisher": {
                                        "publisherMos": {
                                            "mos": 4.1,
                                            "avgJitter": 12.1,
                                            "rtt": 30.5,
                                            "packetsLoss": 0.01
                                        }
                                    }
                                }
                            },
                            "userId": "user789",
                            "userRole": "teacher",
                            "event": "tech-summary-minute",
                            "@timestamp": "2024-01-15T10:31:00Z"
                        }
                    }
                ]
            }
        }
        
        # Сохраняем оригинал для сравнения
        import copy
        original = copy.deepcopy(es_response)
        
        # Обрабатываем через нашу функцию
        processed = process_elasticsearch_data(es_response, "logs_videocall", self.index_config)
        
        # Проверяем что структура сохранилась
        self.assertIn("hits", processed)
        self.assertEqual(len(processed["hits"]["hits"]), 2)
        
        # Проверяем первый документ
        first_doc = processed["hits"]["hits"][0]["_source"]
        self.assertIn("issueReason", first_doc)  # Алиас применился
        self.assertIn("mos", first_doc)  # Алиас применился
        self.assertIn("userId", first_doc)  # Поле без алиаса осталось
        
        # Проверяем дедупликацию
        original_reason = original["hits"]["hits"][0]["_source"]["details"]["issues"]["reason"]
        processed_reason = first_doc["issueReason"]
        self.assertEqual(original_reason, "timeout, connection, timeout, error")
        self.assertEqual(processed_reason, "timeout, connection, error")  # Дубли удалены
        
        # Проверяем второй документ
        second_doc = processed["hits"]["hits"][1]["_source"]
        original_reason2 = original["hits"]["hits"][1]["_source"]["details"]["issues"]["reason"]
        processed_reason2 = second_doc["issueReason"]
        self.assertEqual(original_reason2, "audio, video, audio")
        self.assertEqual(processed_reason2, "audio, video")  # Дубли удалены
        
        print("✅ Полный цикл обработки работает корректно")
    
    def test_index_config_loading(self):
        """Тест загрузки конфигурации индексов"""
        config = load_index_config()
        
        # Проверяем что конфигурация загрузилась
        self.assertIsInstance(config, dict)
        self.assertGreater(len(config), 0)
        
        # Проверяем что есть ожидаемые индексы
        expected_indexes = ["logs_videocall", "logs_tech_summary"]
        for index in expected_indexes:
            if index in config:
                self.assertIn("fields", config[index])
                self.assertIsInstance(config[index]["fields"], dict)
        
        print(f"✅ Конфигурация загружена: {len(config)} индексов")


class TestPerformance(unittest.TestCase):
    """Тесты производительности"""
    
    def test_large_dataset_processing(self):
        """Тест обработки большого набора данных"""
        # Создаем большой набор данных
        large_response = {
            "hits": {
                "total": {"value": 1000},
                "hits": []
            }
        }
        
        # Генерируем 100 документов для теста
        for i in range(100):
            doc = {
                "_source": {
                    "details": {
                        "issues": {
                            "reason": f"error{i % 5}, timeout, error{i % 5}, connection"
                        },
                        "summary": {
                            "publisher": {
                                "publisherMos": {
                                    "mos": 3.0 + (i % 20) * 0.1,
                                    "avgJitter": 10 + (i % 30),
                                    "rtt": 20 + (i % 50)
                                }
                            }
                        }
                    },
                    "userId": f"user{i}",
                    "event": "webrtcIssue" if i % 2 == 0 else "tech-summary-minute",
                    "@timestamp": f"2024-01-15T10:{30 + i % 30}:00Z"
                }
            }
            large_response["hits"]["hits"].append(doc)
        
        # Сохраняем копию для сравнения
        import copy
        original_copy = copy.deepcopy(large_response)
        
        # Измеряем время обработки
        import time
        start_time = time.time()
        
        config = load_index_config()
        processed = process_elasticsearch_data(large_response, "logs_videocall", config)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Проверяем результат
        self.assertIn("hits", processed)
        self.assertEqual(len(processed["hits"]["hits"]), 100)
        
        # Проверяем что дедупликация работает
        first_doc = processed["hits"]["hits"][0]["_source"]
        original_first_doc = original_copy["hits"]["hits"][0]["_source"]
        
        if "issueReason" in first_doc:
            # Должно быть меньше символов из-за дедупликации
            original_reason = original_first_doc["details"]["issues"]["reason"]
            processed_reason = first_doc["issueReason"]
            self.assertLessEqual(len(processed_reason), len(original_reason))
        
        # Проверяем что основные поля присутствуют
        self.assertIn("userId", first_doc)
        
        print(f"✅ Обработка 100 документов заняла {processing_time:.3f} секунд")
        print(f"   Производительность: {100/processing_time:.1f} документов/сек")


if __name__ == "__main__":
    unittest.main(verbosity=2) 
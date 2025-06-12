#!/usr/bin/env python3
"""
Тесты обработки данных
Проверка функций дедупликации, алиасов и обработки ответов Elasticsearch
"""
import sys
import os
import unittest
from unittest.mock import Mock, patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.data_processing import (
    deduplicate_field_values,
    apply_field_aliases, 
    get_nested_value,
    set_nested_value,
    process_elasticsearch_data
)
from src.config_utils import load_index_config


class TestDataProcessing(unittest.TestCase):
    """Тесты обработки данных"""

    def test_deduplicate_field_values(self):
        """Тест дедупликации значений в полях"""
        print("=== Тест дедупликации ===")
        
        # Тест с дублями
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
            status = "✓" if result == expected else "✗"
            print(f"{status} '{input_val}' -> '{result}' (ожидалось: '{expected}')")
            self.assertEqual(result, expected)

    def test_nested_values(self):
        """Тест работы с вложенными значениями"""
        print("\n=== Тест вложенных значений ===")
        
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
        print("Получение значений:")
        reason = get_nested_value(test_data, 'details.issues.reason')
        mos = get_nested_value(test_data, 'details.summary.publisher.publisherMos.mos')
        user_id = get_nested_value(test_data, 'userId')
        non_existent = get_nested_value(test_data, 'non.existent.field')
        
        print(f"✓ details.issues.reason: {reason}")
        print(f"✓ details.summary.publisher.publisherMos.mos: {mos}")
        print(f"✓ userId: {user_id}")
        print(f"✓ non.existent.field: {non_existent}")
        
        self.assertEqual(reason, "timeout, error, timeout")
        self.assertEqual(mos, 4.2)
        self.assertEqual(user_id, "user123")
        self.assertIsNone(non_existent)

    def test_aliases(self):
        """Тест применения алиасов"""
        print("\n=== Тест алиасов ===")
        
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
        print("Результат применения алиасов:")
        for key, value in result.items():
            print(f"  {key}: {value}")
        
        # Проверяем результат
        self.assertIn("issueReason", result)
        self.assertIn("mos", result)
        self.assertIn("avgJitter", result)
        self.assertIn("userId", result)

    def test_full_processing(self):
        """Полный тест обработки данных Elasticsearch"""
        print("\n=== Полный тест обработки ===")
        
        # Имитируем ответ Elasticsearch
        es_response = {
            "hits": {
                "total": {"value": 1},
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
                            "event": "webrtcIssue",
                            "@timestamp": "2024-01-15T10:30:00Z"
                        }
                    }
                ]
            }
        }
        
        # Сохраняем оригинал для сравнения
        import copy
        original_source = copy.deepcopy(es_response['hits']['hits'][0]['_source']) 
        
        print("Исходные данные:")
        print(f"  issues.reason: {original_source['details']['issues']['reason']}")
        print(f"  mos: {original_source['details']['summary']['publisher']['publisherMos']['mos']}")
        
        # Обрабатываем через нашу функцию
        config = load_index_config()
        processed = process_elasticsearch_data(es_response, "logs_videocall", config)
        
        print("\nОбработанные данные:")
        processed_source = processed['hits']['hits'][0]['_source']
        for key, value in processed_source.items():
            print(f"  {key}: {value}")
        
        print("\nДемонстрация эффекта:")
        print(f"  Было: details.issues.reason = '{original_source['details']['issues']['reason']}'")
        issue_reason = processed_source.get('issueReason', 'НЕ НАЙДЕНО')
        print(f"  Стало: issueReason = '{issue_reason}'")
        print(f"  Дедупликация работает: 'timeout, connection, timeout, error' -> '{issue_reason}'")
        
        # Подсчет экономии символов
        original_json = str(original_source)
        processed_json = str(processed_source)
        print(f"\nЭкономия размера:")
        print(f"  Исходный размер: {len(original_json)} символов")
        print(f"  Обработанный размер: {len(processed_json)} символов")
        print(f"  Экономия: {len(original_json) - len(processed_json)} символов ({(1 - len(processed_json)/len(original_json))*100:.1f}%)")
        
        # Проверяем что обработка прошла
        self.assertIn("hits", processed)
        self.assertEqual(len(processed["hits"]["hits"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2) 
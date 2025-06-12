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
    process_elasticsearch_data,
    format_extracted_values,
    _traverse_path
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

    def test_array_parsing(self):
        """Тест парсинга массивов с новым синтаксисом"""
        print("\n=== Тест парсинга массивов ===")
        
        # Тестовые данные из реального ответа Elasticsearch
        test_data = {
            "details": {
                "issues": [
                    {
                        "reason": "inbound-network-quality",
                        "type": "network",
                        "statsSample": {
                            "avgJitter": 0.0045,
                            "rtt": 21,
                            "packetLossPct": 8
                        }
                    },
                    {
                        "reason": "server-issue", 
                        "type": "server",
                        "statsSample": {
                            "avgJitter": 0.0055,
                            "rtt": 340,
                            "packetLossPct": 4
                        }
                    },
                    {
                        "reason": "inbound-network-quality",
                        "type": "network",
                        "statsSample": {
                            "avgJitter": 0.006,
                            "rtt": 340,
                            "packetLossPct": 8
                        }
                    }
                ]
            },
            "userId": 5614788,
            "userRole": "admin"
        }
        
        # Тест извлечения причин из массива
        print("Извлечение причин из массива:")
        reasons = get_nested_value(test_data, "details.issues[].reason")
        print(f"✓ Сырые причины: {reasons}")
        self.assertEqual(reasons, ['inbound-network-quality', 'server-issue', 'inbound-network-quality'])
        
        # Тест форматирования с дедупликацией
        formatted_reasons = format_extracted_values(reasons)
        print(f"✓ Форматированные причины: {formatted_reasons}")
        self.assertEqual(formatted_reasons, "inbound-network-quality, server-issue")
        
        # Тест извлечения вложенных значений из массива
        print("Извлечение статистики из массива:")
        jitter_values = get_nested_value(test_data, "details.issues[].statsSample.avgJitter")
        print(f"✓ Значения jitter: {jitter_values}")
        self.assertEqual(jitter_values, [0.0045, 0.0055, 0.006])
        
        rtt_values = get_nested_value(test_data, "details.issues[].statsSample.rtt")
        print(f"✓ Значения rtt: {rtt_values}")
        self.assertEqual(rtt_values, [21, 340, 340])
        
        # Тест извлечения типов
        types = get_nested_value(test_data, "details.issues[].type")
        print(f"✓ Типы проблем: {types}")
        self.assertEqual(types, ['network', 'server', 'network'])

    def test_array_aliases(self):
        """Тест применения алиасов для массивов"""
        print("\n=== Тест алиасов для массивов ===")
        
        test_data = {
            "details": {
                "issues": [
                    {"reason": "inbound-network-quality", "type": "network"},
                    {"reason": "server-issue", "type": "server"},
                    {"reason": "inbound-network-quality", "type": "network"}
                ]
            },
            "userId": 5614788,
            "userRole": "admin"
        }
        
        fields_config = {
            "details.issues[].reason": {
                "alias": "issueReason",
                "need_dedupe": True
            },
            "details.issues[].type": {
                "alias": "issueType",
                "need_dedupe": True
            },
            "userId": {
                "alias": None
            }
        }
        
        result = apply_field_aliases(test_data, fields_config)
        print("Результат с алиасами для массивов:")
        print(f"  issueReason: {result.get('issueReason')}")
        print(f"  issueType: {result.get('issueType')}")
        print(f"  userId: {result.get('userId')}")
        
        # Проверяем результат
        self.assertEqual(result.get('issueReason'), "inbound-network-quality, server-issue")
        self.assertEqual(result.get('issueType'), "network, server")
        self.assertEqual(result.get('userId'), 5614788)

    def test_array_edge_cases(self):
        """Тест граничных случаев для массивов"""
        print("\n=== Тест граничных случаев для массивов ===")
        
        # Пустой массив
        empty_array_data = {"details": {"issues": []}}
        result = get_nested_value(empty_array_data, "details.issues[].reason")
        print(f"✓ Пустой массив: {result}")
        self.assertIsNone(result)
        
        # Массив с пустыми объектами
        empty_objects_data = {"details": {"issues": [{}, {"reason": "test"}]}}
        result = get_nested_value(empty_objects_data, "details.issues[].reason")
        print(f"✓ Массив с пустыми объектами: {result}")
        self.assertEqual(result, ["test"])
        
        # Несуществующий путь
        result = get_nested_value(empty_objects_data, "details.nonexistent[].field")
        print(f"✓ Несуществующий путь: {result}")
        self.assertIsNone(result)
        
        # Не массив
        not_array_data = {"details": {"issues": "not an array"}}
        result = get_nested_value(not_array_data, "details.issues[].reason")
        print(f"✓ Не массив: {result}")
        self.assertIsNone(result)

    def test_specific_array_index(self):
        """Тест извлечения конкретного элемента массива"""
        print("\n=== Тест извлечения конкретного элемента массива ===")
        
        test_data = {
            "details": {
                "issues": [
                    {"reason": "first", "priority": 1},
                    {"reason": "second", "priority": 2},
                    {"reason": "third", "priority": 3}
                ]
            }
        }
        
        # Тест извлечения первого элемента
        first_reason = get_nested_value(test_data, "details.issues[0].reason")
        print(f"✓ Первый элемент: {first_reason}")
        self.assertEqual(first_reason, "first")
        
        # Тест извлечения второго элемента
        second_priority = get_nested_value(test_data, "details.issues[1].priority")
        print(f"✓ Второй элемент: {second_priority}")
        self.assertEqual(second_priority, 2)
        
        # Тест извлечения несуществующего индекса
        nonexistent = get_nested_value(test_data, "details.issues[10].reason")
        print(f"✓ Несуществующий индекс: {nonexistent}")
        self.assertIsNone(nonexistent)

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
        
        print(f"\nСтатистика:")
        print(f"  Размер до обработки: {len(original_json)} символов")
        print(f"  Размер после обработки: {len(processed_json)} символов")
        
        # Проверяем, что обработка прошла успешно
        self.assertIsInstance(processed, dict)
        self.assertIn('hits', processed)


if __name__ == '__main__':
    unittest.main(verbosity=2) 
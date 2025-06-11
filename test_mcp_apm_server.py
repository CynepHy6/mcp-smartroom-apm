#!/usr/bin/env python3
"""
Unit тесты для MCP APM сервера
Проверка исправленной логики фильтрации плоских полей
"""

import pytest
import sys
import json

sys.path.insert(0, '.')
from mcp_apm_server import filter_source_fields, get_configured_fields


class TestFixedImplementation:
    """Тест исправленной реализации фильтрации"""
    
    def test_fixed_filter_function_with_flat_fields(self):
        """Тест исправленной функции filter_source_fields с плоскими полями"""
        source = {
            "appSessionId": "session123",
            "event": "tech-summary-minute",
            "@timestamp": "2023-12-01T10:00:00Z",
            "userId": "user456",
            "userRole": "student",
            "details.summary.publisher.publisherMos.mos": 4.2,
            "details.summary.publisher.publisherMos.avgJitter": 10.5,
            "details.summary.publisher.publisherMos.packetsLoss": 0.1,
            "details.summary.publisher.publisherMos.rtt": 50.0,
            "details.issues.reason": "network_issue",
            "unwanted_field1": "remove_me",
            "unwanted_field2": "also_remove_me"
        }
        
        allowed_fields = [
            "appSessionId",
            "event",
            "@timestamp",
            "userId",
            "userRole",
            "details.issues.reason",
            "details.summary.publisher.publisherMos.avgJitter",
            "details.summary.publisher.publisherMos.mos",
            "details.summary.publisher.publisherMos.packetsLoss",
            "details.summary.publisher.publisherMos.rtt"
        ]
        
        result = filter_source_fields(source, allowed_fields)
        
        expected = {
            "appSessionId": "session123",
            "event": "tech-summary-minute",
            "@timestamp": "2023-12-01T10:00:00Z",
            "userId": "user456",
            "userRole": "student",
            "details.summary.publisher.publisherMos.mos": 4.2,
            "details.summary.publisher.publisherMos.avgJitter": 10.5,
            "details.summary.publisher.publisherMos.packetsLoss": 0.1,
            "details.summary.publisher.publisherMos.rtt": 50.0,
            "details.issues.reason": "network_issue"
        }
        
        assert result == expected
        assert "unwanted_field1" not in result
        assert "unwanted_field2" not in result
        
        print("✅ Исправленная функция работает корректно!")
        print(f"Исходный размер: {len(str(source))} символов")
        print(f"Отфильтрованный размер: {len(str(result))} символов")
        print(f"Удалено полей: {len(source) - len(result)}")
    
    def test_get_configured_fields_function(self):
        """Тест функции get_configured_fields"""
        fields = get_configured_fields("logs_videocall")
        
        print(f"Найденные поля для logs_videocall: {fields}")
        
        expected_fields = [
            "appSessionId",
            "event", 
            "@timestamp",
            "userId",
            "userRole",
            "details.issues.reason",
            "details.summary.publisher.publisherMos.avgJitter",
            "details.summary.publisher.publisherMos.mos",
            "details.summary.publisher.publisherMos.packetsLoss",
            "details.summary.publisher.publisherMos.rtt"
        ]
        
        assert len(fields) > 0, "Должны быть найдены поля из конфигурации"
        
        for expected_field in expected_fields:
            assert expected_field in fields, f"Поле {expected_field} должно быть в конфигурации"
        
        print("✅ Функция get_configured_fields работает корректно!")
    
    def test_real_world_filtering(self):
        """Тест фильтрации на реальных данных видеозвонка"""
        real_log = {
            "appSessionId": "abc123def456", 
            "event": "tech-summary-minute",
            "@timestamp": "2023-12-01T14:30:15.123Z",
            "userId": "student_12345",
            "userRole": "student",
            "sessionId": "session_789",
            "lessonId": "lesson_456", 
            "details.summary.publisher.publisherMos.mos": 3.8,
            "details.summary.publisher.publisherMos.avgJitter": 12.3,
            "details.summary.publisher.publisherMos.packetsLoss": 0.02,
            "details.summary.publisher.publisherMos.rtt": 45.7,
            "details.summary.subscriber.subscriberMos.mos": 3.9,
            "details.summary.subscriber.subscriberMos.avgJitter": 8.1,
            "details.issues.reason": "network_fluctuation",
            "details.issues.severity": "medium",
            "technical.browserVersion": "Chrome/119.0.0.0",
            "technical.osVersion": "Windows 10",
            "technical.networkType": "wifi",
            "metadata.courseId": "course_123",
            "metadata.teacherId": "teacher_789",
            "system.processingTime": 150,
            "system.queueDepth": 5
        }
        
        configured_fields = get_configured_fields("logs_videocall")
        filtered = filter_source_fields(real_log, configured_fields)
        
        original_size = len(json.dumps(real_log))
        filtered_size = len(json.dumps(filtered))
        compression_ratio = (original_size - filtered_size) / original_size * 100
        
        print(f"Реальный лог - оригинал: {original_size} символов")
        print(f"Реальный лог - отфильтрован: {filtered_size} символов")
        print(f"Сжатие: {compression_ratio:.1f}%")
        
        assert "appSessionId" in filtered
        assert "details.summary.publisher.publisherMos.mos" in filtered
        assert "details.issues.reason" in filtered
        
        assert "sessionId" not in filtered
        assert "technical.browserVersion" not in filtered
        assert "metadata.courseId" not in filtered
        
        assert compression_ratio > 30
        
        print("✅ Реальная фильтрация работает эффективно!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"]) 
#!/usr/bin/env python3
"""
Клиент для работы с Elasticsearch
"""

import os
import logging
from typing import Dict, Any, List, Optional
from elasticsearch import AsyncElasticsearch, exceptions as es_exceptions

logger = logging.getLogger(__name__)

class ElasticsearchManager:
    """Менеджер для работы с Elasticsearch"""
    
    def __init__(self):
        self.client = AsyncElasticsearch(
            hosts=[os.getenv("APM_BASE_URL")],
            basic_auth=(os.getenv("APM_USERNAME"), os.getenv("APM_PASSWORD")),
            request_timeout=int(os.getenv("APM_TIMEOUT", "30"))
        )

    async def list_indexes(self, index_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Список индексов и их описание"""
        result = []
        
        for idx, data in index_config.items():
            fields_info = {}
            events = data.get("events", [])
            fields = data.get("fields", {})
            
            # Обрабатываем поля из раздела fields
            for field_name, field_config in fields.items():
                alias = field_config.get("alias")
                display_name = alias if alias else field_name
                description = field_config.get("description", "")
                
                fields_info[display_name] = {
                    "original_name": field_name,
                    "description": description,
                    "alias": alias,
                    "need_dedupe": field_config.get("need_dedupe", False)
                }
            
            result.append({
                "name": idx,
                "fields": fields_info,
                "events": events
            })
        return result

    async def query_index(self, index: str, filters: Dict[str, Any], size: int = 100, 
                         from_: int = 0, sort: Optional[List[Dict]] = None, 
                         index_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Выполняет запрос к индексу Elasticsearch"""
        if index_config and index not in index_config:
            raise ValueError(f"Индекс '{index}' не найден в конфигурации")
        
        query_body = self._build_query(filters, size, from_, sort)
        
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
    
    def _build_query(self, filters: Dict[str, Any], size: int, from_: int, 
                    sort: Optional[List[Dict]]) -> Dict[str, Any]:
        """Строит тело запроса к Elasticsearch"""
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
            
        return query_body 
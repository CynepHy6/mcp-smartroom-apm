#!/usr/bin/env python3
"""
Модуль для создания графиков на основе данных из Elasticsearch
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from .data_processing import get_nested_value

logger = logging.getLogger(__name__)

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import pandas as pd
    import numpy as np
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    logger.warning("Matplotlib/pandas не установлены. Функция построения графиков недоступна.")

class PlotManager:
    """Менеджер для создания графиков"""
    
    def __init__(self):
        self.plots_dir = Path("plots")
    
    def is_available(self) -> bool:
        """Проверяет доступность функции построения графиков"""
        return PLOTTING_AVAILABLE
    
    async def create_plot_from_data(self, es_result: Dict[str, Any], plot_type: str, 
                                  x_field: str, y_field: str, group_by: Optional[str], 
                                  title: str) -> str:
        """Создает график по данным из Elasticsearch и сохраняет в файл"""
        if not PLOTTING_AVAILABLE:
            return "Ошибка: matplotlib/pandas не установлены"
        
        try:
            records = self._extract_records(es_result, x_field, y_field, group_by)
            
            if not records:
                return "Нет данных для построения графика"
            
            df = pd.DataFrame(records)
            
            # Обрабатываем временные данные
            if x_field == '@timestamp' or 'timestamp' in x_field.lower():
                df['x'] = pd.to_datetime(df['x'])
            
            file_path = self._create_and_save_plot(
                df, plot_type, x_field, y_field, group_by, title
            )
            
            result = {
                "status": "success",
                "file_path": str(file_path.absolute()),
                "file_size": file_path.stat().st_size,
                "message": f"График создан успешно и сохранен. ВАЖНО: покажи пользователю в ответе путь к графику 'file_path', чтобы он мог его посмотреть"
            }
            
            return json.dumps(result, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logger.error(f"Ошибка создания графика: {e}")
            return f"Ошибка создания графика: {str(e)}"
    
    def _extract_records(self, es_result: Dict[str, Any], x_field: str, 
                        y_field: str, group_by: Optional[str]) -> list:
        """Извлекает записи из результата Elasticsearch"""
        records = []
        string_to_numeric_map = {}
        numeric_counter = 1
        
        for hit in es_result.get('hits', {}).get('hits', []):
            source = hit['_source']
            
            x_value = get_nested_value(source, x_field) if '.' in x_field else source.get(x_field)
            y_value = get_nested_value(source, y_field) if '.' in y_field else source.get(y_field)
            group_value = get_nested_value(source, group_by) if group_by and '.' in group_by else source.get(group_by) if group_by else None
            
            if x_value is not None and y_value is not None:
                # Преобразуем строковые Y значения в числовые для графиков
                if isinstance(y_value, str):
                    if y_value not in string_to_numeric_map:
                        string_to_numeric_map[y_value] = numeric_counter
                        numeric_counter += 1
                    y_numeric = string_to_numeric_map[y_value]
                else:
                    y_numeric = y_value
                
                records.append({
                    'x': x_value,
                    'y': y_numeric,
                    'y_original': y_value,  # Сохраняем оригинальное значение
                    'group': group_value
                })
        
        # Сохраняем маппинг для использования в подписях
        self._string_mapping = string_to_numeric_map
        return records
    
    def _create_and_save_plot(self, df, plot_type: str, x_field: str, y_field: str, 
                             group_by: Optional[str], title: str) -> tuple:
        """Создает график и сохраняет в файл"""
        plt.figure(figsize=(12, 8))
        
        self._render_plot(df, plot_type, group_by)
        
        # Настройка осей и заголовков
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(x_field, fontsize=12)
        plt.ylabel(y_field, fontsize=12)
        
        # Форматирование временной оси
        if x_field == '@timestamp' or 'timestamp' in x_field.lower():
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            plt.xticks(rotation=45)
        
        # Если есть маппинг строк в числа, настраиваем подписи оси Y
        if hasattr(self, '_string_mapping') and self._string_mapping:
            y_ticks = list(self._string_mapping.values())
            y_labels = list(self._string_mapping.keys())
            plt.yticks(y_ticks, y_labels)
        
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        file_path = self._generate_filename(title)
        
        # Сохраняем график в файл
        plt.savefig(file_path, format='png', dpi=150, bbox_inches='tight')
        plt.close()
        
        return file_path
    
    def _render_plot(self, df, plot_type: str, group_by: Optional[str]):
        """Отрисовывает график в зависимости от типа"""
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        
        if plot_type == "mos_timeline":
            self._render_mos_timeline(df, group_by, colors)
        elif plot_type == "line":
            self._render_line_plot(df, group_by, colors)
        elif plot_type == "scatter":
            self._render_scatter_plot(df, group_by, colors)
        elif plot_type == "bar":
            self._render_bar_plot(df, group_by)
    
    def _render_mos_timeline(self, df, group_by: Optional[str], colors: list):
        """Отрисовывает специальный график для МОС"""
        if group_by:
            groups = df['group'].unique()
            for i, group in enumerate(groups):
                group_data = df[df['group'] == group].sort_values('x')
                plt.plot(group_data['x'], group_data['y'], 
                        marker='o', markersize=4, linewidth=2,
                        color=colors[i % len(colors)],
                        label=f'{group_by}: {group}')
            plt.legend()
        else:
            plt.plot(df['x'], df['y'], marker='o', markersize=4, linewidth=2)
        
        plt.ylim(3.5, 5.0)
        plt.axhline(y=4.0, color='red', linestyle='--', alpha=0.5, label='Хорошее качество (4.0)')
        plt.axhline(y=4.5, color='green', linestyle='--', alpha=0.5, label='Отличное качество (4.5)')
    
    def _render_line_plot(self, df, group_by: Optional[str], colors: list):
        """Отрисовывает линейный график"""
        if group_by:
            groups = df['group'].unique()
            for i, group in enumerate(groups):
                group_data = df[df['group'] == group].sort_values('x')
                plt.plot(group_data['x'], group_data['y'], 
                        marker='o', markersize=3, linewidth=1.5,
                        color=colors[i % len(colors)],
                        label=f'{group_by}: {group}')
            plt.legend()
        else:
            plt.plot(df['x'], df['y'], marker='o', markersize=3, linewidth=1.5)
    
    def _render_scatter_plot(self, df, group_by: Optional[str], colors: list):
        """Отрисовывает точечный график"""
        if group_by:
            groups = df['group'].unique()
            for i, group in enumerate(groups):
                group_data = df[df['group'] == group]
                plt.scatter(group_data['x'], group_data['y'], 
                          color=colors[i % len(colors)],
                          label=f'{group_by}: {group}', alpha=0.7)
            plt.legend()
        else:
            plt.scatter(df['x'], df['y'], alpha=0.7)
    
    def _render_bar_plot(self, df, group_by: Optional[str]):
        """Отрисовывает столбчатый график"""
        if group_by:
            grouped = df.groupby('group')['y'].mean()
            plt.bar(range(len(grouped)), grouped.values)
            plt.xticks(range(len(grouped)), grouped.index, rotation=45)
        else:
            grouped = df.groupby('x')['y'].mean()
            plt.bar(range(len(grouped)), grouped.values)
            plt.xticks(range(len(grouped)), grouped.index, rotation=45)
    
    def _generate_filename(self, title: str) -> Path:
        """Генерирует уникальное имя файла"""
        now = datetime.now()
        
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_title = safe_title.replace(' ', '_')[:40]
        
        filename = f"{now.strftime('%H:%M:%S')}_{safe_title}.png"
        
        year = now.strftime("%Y")
        month = now.strftime("%m")
        plots_dir = self.plots_dir / year / month
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        return plots_dir / filename
    
    def _calculate_statistics(self, df, group_by: Optional[str]) -> Dict[str, Any]:
        """Вычисляет статистику по данным"""
        stats = {
            "total_points": len(df),
            "x_range": [str(df['x'].min()), str(df['x'].max())],
            "y_range": [float(df['y'].min()), float(df['y'].max())],
            "y_mean": float(df['y'].mean()),
            "y_std": float(df['y'].std())
        }
        
        if group_by:
            stats["groups"] = df['group'].unique().tolist()
        
        return stats 
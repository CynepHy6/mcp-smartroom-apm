#!/usr/bin/env python3
"""
Кроссплатформенный скрипт установки MCP Smartroom APM сервера
Работает на Linux, macOS и Windows
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

def run_command(cmd, cwd=None):
    """Выполнить команду и вернуть результат"""
    try:
        result = subprocess.run(cmd, shell=True, check=True, cwd=cwd, 
                              capture_output=True, text=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def main():
    print("=== Установка MCP Smartroom APM сервера ===")
    
    script_dir = Path(__file__).parent.absolute()
    print(f"Директория проекта: {script_dir}")
    
    os.chdir(script_dir)
    
    system = platform.system()
    print(f"Операционная система: {system}")
    
    venv_dir = script_dir / "venv"
    venv_python = venv_dir / ("Scripts" if system == "Windows" else "bin") / ("python.exe" if system == "Windows" else "python")
    venv_pip = venv_dir / ("Scripts" if system == "Windows" else "bin") / ("pip.exe" if system == "Windows" else "pip")
    
    if not venv_dir.exists():
        print("Создание виртуального окружения...")
        success, output = run_command(f"{sys.executable} -m venv venv")
        if not success:
            print(f"Ошибка создания виртуального окружения: {output}")
            sys.exit(1)
    else:
        print("Виртуальное окружение уже существует")
    
    print("Обновление pip...")
    success, output = run_command(f'"{venv_pip}" install --upgrade pip')
    if not success:
        print(f"Предупреждение: не удалось обновить pip: {output}")
    
    print("Установка зависимостей...")
    success, output = run_command(f'"{venv_pip}" install -r requirements.txt')
    if not success:
        print(f"Ошибка установки зависимостей: {output}")
        sys.exit(1)
    
    config_files = {
        "index.yaml": "файл конфигурации индексов",
        ".env": "файл настроек Elasticsearch"
    }
    
    for filename, description in config_files.items():
        if not (script_dir / filename).exists():
            print(f"Предупреждение: {filename} не найден ({description})")
    
    if not (script_dir / ".env").exists():
        print("\nСоздайте файл .env с настройками Elasticsearch:")
        print("APM_BASE_URL=https://your-elasticsearch-url")
        print("APM_USERNAME=your-username")
        print("APM_PASSWORD=your-password") 
        print("APM_TIMEOUT=30")
    
    if system == "Windows":
        create_windows_bat(script_dir, venv_python)
    
    print("\n=== Установка завершена ===")
    
    if system == "Windows":
        print("Для запуска используйте: start_server.bat")
        print("Или: python mcp-server")
    else:
        print("Для запуска используйте: python3 mcp-server")
        print("Или: ./mcp-server (после chmod +x mcp-server)")

def create_windows_bat(script_dir, venv_python):
    """Создает bat-файл для Windows"""
    bat_content = f'''@echo off
cd /d "{script_dir}"
"{venv_python}" "{script_dir / 'mcp-apm-server.py'}" %*
pause
'''
    bat_path = script_dir / "start_server.bat"
    with open(bat_path, "w", encoding="utf-8") as f:
        f.write(bat_content)

if __name__ == "__main__":
    main() 
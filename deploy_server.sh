#!/bin/bash

# Скрипт для автоматического развертывания проекта на сервере
# Использование: ./deploy_server.sh

set -e  # Останавливаем выполнение при любой ошибке

echo "Начинаю развертывание проекта Callchecker..."

# Проверяем что мы в правильной директории
if [ ! -f "requirements.txt" ]; then
    echo "Ошибка: файл requirements.txt не найден. Убедитесь что вы в корневой директории проекта."
    exit 1
fi

# 1. Обновляем код из Git
echo "Обновляю код из Git..."
git pull origin main

# 2. Активируем виртуальное окружение
echo "Активирую виртуальное окружение..."
if [ ! -d "venv" ]; then
    echo "Создаю виртуальное окружение..."
    python3 -m venv venv
fi
source venv/bin/activate

# 3. Обновляем зависимости
echo "Обновляю зависимости..."
pip install --upgrade pip
pip install -r requirements.txt

# 4. Копируем env файл если его нет
if [ ! -f ".env" ]; then
    echo "Создаю .env файл..."
    if [ -f "env.production" ]; then
        cp env.production .env
        echo "Скопирован env.production в .env"
    else
        echo "Файл env.production не найден. Создайте его с правильными настройками."
        exit 1
    fi
else
    echo "Файл .env уже существует"
fi

# 5. Проверяем Google Sheets credentials
if [ ! -f "bitrix24/google_sheets_credentials.json" ]; then
    echo "Файл bitrix24/google_sheets_credentials.json не найден!"
    echo "Скопируйте его с локального компьютера:"
    echo "   scp bitrix24/google_sheets_credentials.json USER@SERVER:~/callchecker/bitrix24/"
    exit 1
else
    echo "Google Sheets credentials найдены"
fi

# 6. Проверяем подключение к базе данных
echo "Проверяю подключение к базе данных..."
python -c "
from db_client import get_db_client
try:
    conn = get_db_client()
    print('Подключение к PostgreSQL успешно!')
    conn.close()
except Exception as e:
    print(f'Ошибка подключения к БД: {e}')
    exit(1)
"

# 7. Проверяем Google Sheets подключение
echo "Проверяю Google Sheets подключение..."
python -c "
import gspread
from google.oauth2.service_account import Credentials
try:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_file('bitrix24/google_sheets_credentials.json', scopes=scopes)
    client = gspread.authorize(credentials)
    print('Google Sheets подключение успешно!')
except Exception as e:
    print(f'Ошибка Google Sheets: {e}')
    exit(1)
"

# 8. Создаем необходимые директории
echo "Создаю директории для логов..."
mkdir -p logs

echo ""
echo "Развертывание завершено успешно!"
echo ""
echo "Доступные команды для запуска сервисов:"
echo "   python bitrix24/main.py                    # Основной сервис Bitrix24"
echo "   python dialogue_recognition.py            # Распознавание речи"
echo "   python dialog_analysis.py                 # Анализ диалогов"
echo "   python google_sheet/google_sheets_synchronizer.py  # Синхронизация Google Sheets"
echo ""
echo "Управляющие команды:"
echo "   python bitrix24/setup_portals.py          # Создание таблиц БД"
echo "   python bitrix24/sync_categories_criteria.py  # Синхронизация критериев"
echo ""
echo "Мониторинг:"
echo "   tail -f logs/*.log                        # Просмотр логов"
echo "   ps aux | grep python                      # Запущенные процессы"
echo ""

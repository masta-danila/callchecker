#!/bin/bash

# Скрипт для копирования необходимых файлов на сервер
# Запускать с ЛОКАЛЬНОГО компьютера
# Использование: ./copy_files_to_server.sh

SERVER_USER="callchecker"
SERVER_IP="135.181.242.20"
SERVER_PATH="~/callchecker"

echo "Копирую файлы на сервер $SERVER_IP..."

# 1. Копируем env.production как .env
echo "Копирую env.production..."
if [ -f "env.production" ]; then
    scp env.production $SERVER_USER@$SERVER_IP:$SERVER_PATH/.env
    echo "env.production скопирован как .env"
else
    echo "Файл env.production не найден!"
    exit 1
fi

# 2. Копируем Google Sheets credentials
echo "Копирую Google Sheets credentials..."
if [ -f "bitrix24/google_sheets_credentials.json" ]; then
    scp bitrix24/google_sheets_credentials.json $SERVER_USER@$SERVER_IP:$SERVER_PATH/bitrix24/
    echo "Google Sheets credentials скопированы"
else
    echo "Файл bitrix24/google_sheets_credentials.json не найден!"
    exit 1
fi

# 3. Копируем скрипт развертывания
echo "Копирую скрипт развертывания..."
scp deploy_server.sh $SERVER_USER@$SERVER_IP:$SERVER_PATH/
ssh $SERVER_USER@$SERVER_IP "chmod +x $SERVER_PATH/deploy_server.sh"
echo "Скрипт развертывания скопирован"

echo ""
echo "Все файлы скопированы на сервер!"
echo ""
echo "Теперь на сервере выполните:"
echo "   ssh $SERVER_USER@$SERVER_IP"
echo "   cd callchecker"
echo "   ./deploy_server.sh"
echo ""

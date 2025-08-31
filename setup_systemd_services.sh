#!/bin/bash

# Скрипт для установки systemd сервисов Callchecker
set -e

echo "🔧 Устанавливаю systemd сервисы Callchecker..."

# Копируем service файлы в systemd директорию
echo "📁 Копирую service файлы..."
sudo cp callchecker-*.service /etc/systemd/system/

# Перезагружаем systemd
echo "🔄 Перезагружаю systemd daemon..."
sudo systemctl daemon-reload

# Включаем автозапуск всех сервисов
echo "⚡ Включаю автозапуск сервисов..."
sudo systemctl enable callchecker-bitrix.service
sudo systemctl enable callchecker-recognition.service
sudo systemctl enable callchecker-analysis.service
sudo systemctl enable callchecker-sheets.service

# Запускаем все сервисы
echo "🚀 Запускаю все сервисы..."
sudo systemctl start callchecker-bitrix.service
sudo systemctl start callchecker-recognition.service
sudo systemctl start callchecker-analysis.service
sudo systemctl start callchecker-sheets.service

# Проверяем статус
echo "📊 Статус сервисов:"
sudo systemctl status callchecker-bitrix.service --no-pager -l
sudo systemctl status callchecker-recognition.service --no-pager -l
sudo systemctl status callchecker-analysis.service --no-pager -l
sudo systemctl status callchecker-sheets.service --no-pager -l

echo "✅ Все сервисы установлены и запущены!"
echo ""
echo "📝 Полезные команды:"
echo "   sudo systemctl status callchecker-*     # Статус всех сервисов"
echo "   sudo systemctl restart callchecker-*    # Перезапуск всех сервисов"
echo "   sudo systemctl stop callchecker-*       # Остановка всех сервисов"
echo "   sudo journalctl -u callchecker-bitrix -f # Логи конкретного сервиса"

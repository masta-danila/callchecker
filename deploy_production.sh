#!/bin/bash

# Production Deployment Script for Callchecker
# Использование: ./deploy_production.sh

set -e  # Остановка скрипта при любой ошибке

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функции для цветного вывода
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка что скрипт не запущен от root
if [ "$EUID" -eq 0 ]; then
    log_error "Не запускайте этот скрипт от имени root!"
    log_info "Создайте отдельного пользователя: sudo ./setup_server.sh"
    exit 1
fi

# Проверка доступности Docker
if ! command -v docker &> /dev/null; then
    log_error "Docker не установлен! Установите Docker перед продолжением."
    exit 1
fi

# Проверка что Docker запущен
if ! docker info &> /dev/null; then
    log_error "Docker daemon не запущен или нет прав доступа!"
    log_info "Попробуйте: sudo systemctl start docker"
    log_info "Или добавьте пользователя в группу docker: sudo usermod -aG docker \$USER"
    exit 1
fi

log_info "🚀 Начинаю развертывание Callchecker в production..."

# Проверка существования файла окружения
if [ ! -f "env.production" ]; then
    log_error "Файл env.production не найден!"
    log_info "Создайте его на основе env.example"
    exit 1
fi

# Копирование переменных окружения
log_info "📝 Настройка переменных окружения..."
cp env.production .env
log_success "Переменные окружения настроены"

# Остановка существующих контейнеров
log_info "🛑 Остановка существующих контейнеров..."
docker compose down --remove-orphans || true

# Очистка старых образов (опционально)
read -p "Очистить старые Docker образы? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "🧹 Очистка старых образов..."
    docker system prune -f
    docker image prune -f
fi

# Сборка образов
log_info "🔨 Сборка Docker образов..."
docker compose build --no-cache

# Проверка свободного места
log_info "💾 Проверка свободного места на диске..."
df -h | grep -E '(Filesystem|/$)'

# Создание необходимых директорий на хосте
log_info "📁 Создание директорий для volumes..."
sudo mkdir -p /var/lib/docker/volumes/callchecker_postgres_data
sudo mkdir -p /var/lib/docker/volumes/callchecker_redis_data
sudo mkdir -p /var/lib/docker/volumes/callchecker_app_logs
sudo mkdir -p /var/lib/docker/volumes/callchecker_audio_storage

# Запуск сервисов в production режиме
log_info "🚀 Запуск сервисов..."
docker compose up -d

# Ожидание запуска базы данных
log_info "⏳ Ожидание запуска PostgreSQL..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker compose exec postgres pg_isready -U callchecker -d callchecker_db &> /dev/null; then
        log_success "PostgreSQL запущена!"
        break
    fi
    echo -n "."
    sleep 2
    ((timeout-=2))
done

if [ $timeout -le 0 ]; then
    log_error "PostgreSQL не запустилась в течение 60 секунд!"
    docker compose logs postgres
    exit 1
fi

# Ожидание запуска Redis
log_info "⏳ Ожидание запуска Redis..."
timeout=30
while [ $timeout -gt 0 ]; do
    if docker compose exec redis redis-cli ping &> /dev/null; then
        log_success "Redis запущен!"
        break
    fi
    echo -n "."
    sleep 2
    ((timeout-=2))
done

# Проверка статуса всех сервисов
log_info "📊 Проверка статуса сервисов..."
sleep 10
docker compose ps

# Проверка логов на наличие ошибок
log_info "📋 Проверка логов на ошибки..."
if docker compose logs --tail=50 | grep -i error; then
    log_warning "Обнаружены ошибки в логах! Проверьте выше."
else
    log_success "Критических ошибок в логах не найдено"
fi

# Информация о развертывании
log_success "🎉 Развертывание завершено!"
echo
log_info "📊 Полезные команды для управления:"
echo "  Посмотреть статус:     docker compose ps"
echo "  Посмотреть логи:       docker compose logs -f [service_name]"
echo "  Остановить:            docker compose down"
echo "  Перезапустить:         docker compose restart [service_name]"
echo "  Обновить:              git pull && docker compose build && docker compose up -d"
echo
log_info "📋 Мониторинг:"
echo "  Все логи:              docker compose logs -f"
echo "  База данных:           docker compose logs -f postgres"
echo "  Основной сервис:       docker compose logs -f bitrix-main"
echo "  Распознавание:         docker compose logs -f dialogue-recognition"
echo "  Анализ:                docker compose logs -f dialog-analysis"
echo "  Google Sheets:         docker compose logs -f google-sheets-sync"
echo
log_info "🔧 Настройка:"
echo "  Настройка порталов:    docker compose exec bitrix-main python bitrix24/setup_portals.py"
echo "  Синхронизация критериев: docker compose exec bitrix-main python bitrix24/sync_categories_criteria.py"
echo
log_warning "⚠️  Не забудьте:"
echo "  1. Настроить webhook URL в Bitrix24"
echo "  2. Добавить Google Sheets credentials если используете интеграцию"
echo "  3. Настроить файрволл для портов 5432, 6379 (только для локального доступа)"
echo "  4. Настроить регулярные бэкапы базы данных"

log_success "✅ Callchecker успешно развернут и готов к работе!"

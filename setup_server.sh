#!/bin/bash

# Server Setup Script for Callchecker
# Запускайте от имени root: sudo ./setup_server.sh

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

# Проверка что скрипт запущен от root
if [ "$EUID" -ne 0 ]; then
    log_error "Этот скрипт должен быть запущен от имени root!"
    log_info "Используйте: sudo ./setup_server.sh"
    exit 1
fi

log_info "🔧 Настройка сервера для Callchecker..."

# Обновление системы
log_info "📦 Обновление пакетов системы..."
apt update
apt upgrade -y

# Установка необходимых пакетов
log_info "📋 Установка необходимых пакетов..."
apt install -y \
    curl \
    wget \
    git \
    htop \
    tree \
    nano \
    unzip \
    software-properties-common \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release

# Установка Docker
log_info "🐳 Установка Docker..."
if ! command -v docker &> /dev/null; then
    # Добавление официального GPG ключа Docker
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    
    # Добавление репозитория Docker
    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Установка Docker Engine
    apt update
    apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    
    # Запуск и автозагрузка Docker
    systemctl start docker
    systemctl enable docker
    
    log_success "Docker успешно установлен!"
else
    log_info "Docker уже установлен"
fi

# Создание пользователя для приложения
USERNAME="callchecker"
if id "$USERNAME" &>/dev/null; then
    log_info "Пользователь $USERNAME уже существует"
else
    log_info "👤 Создание пользователя $USERNAME..."
    useradd -m -s /bin/bash "$USERNAME"
    
    # Добавление пользователя в группу docker
    usermod -aG docker "$USERNAME"
    
    log_success "Пользователь $USERNAME создан и добавлен в группу docker"
fi

# Создание SSH ключей для пользователя (опционально)
CALLCHECKER_HOME="/home/$USERNAME"
if [ ! -f "$CALLCHECKER_HOME/.ssh/id_rsa" ]; then
    log_info "🔑 Создание SSH ключей для пользователя $USERNAME..."
    sudo -u "$USERNAME" mkdir -p "$CALLCHECKER_HOME/.ssh"
    sudo -u "$USERNAME" ssh-keygen -t rsa -b 4096 -f "$CALLCHECKER_HOME/.ssh/id_rsa" -N ""
    chmod 700 "$CALLCHECKER_HOME/.ssh"
    chmod 600 "$CALLCHECKER_HOME/.ssh/id_rsa"
    chmod 644 "$CALLCHECKER_HOME/.ssh/id_rsa.pub"
    chown -R "$USERNAME:$USERNAME" "$CALLCHECKER_HOME/.ssh"
    log_success "SSH ключи созданы"
fi

# Создание рабочей директории
WORK_DIR="/home/$USERNAME/callchecker"
if [ ! -d "$WORK_DIR" ]; then
    log_info "📁 Создание рабочей директории..."
    sudo -u "$USERNAME" mkdir -p "$WORK_DIR"
    log_success "Рабочая директория создана: $WORK_DIR"
fi

# Настройка firewall (UFW)
log_info "🔥 Настройка файрволла..."
if command -v ufw &> /dev/null; then
    ufw --force enable
    
    # Разрешить SSH
    ufw allow ssh
    
    # Разрешить HTTP/HTTPS (если планируется веб-интерфейс)
    ufw allow 80/tcp
    ufw allow 443/tcp
    
    # PostgreSQL и Redis только для локальных подключений
    ufw allow from 127.0.0.1 to any port 5432
    ufw allow from 127.0.0.1 to any port 6379
    
    log_success "Файрволл настроен"
else
    log_warning "UFW не найден, файрволл не настроен"
fi

# Настройка логротации
log_info "📄 Настройка ротации логов..."
cat > /etc/logrotate.d/callchecker << EOF
/home/callchecker/callchecker/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 0644 callchecker callchecker
    copytruncate
}
EOF
log_success "Ротация логов настроена"

# Создание systemd сервиса для автозапуска (опционально)
log_info "⚙️  Создание systemd сервиса..."
cat > /etc/systemd/system/callchecker.service << EOF
[Unit]
Description=Callchecker Application
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=true
WorkingDirectory=/home/callchecker/callchecker
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
User=callchecker
Group=callchecker

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
log_success "Systemd сервис создан (не активирован)"

# Информация о завершении
log_success "🎉 Настройка сервера завершена!"
echo
log_info "📋 Что было настроено:"
echo "  ✅ Система обновлена"
echo "  ✅ Docker установлен и настроен"
echo "  ✅ Пользователь '$USERNAME' создан"
echo "  ✅ SSH ключи сгенерированы"
echo "  ✅ Рабочая директория создана: $WORK_DIR"
echo "  ✅ Файрволл настроен"
echo "  ✅ Ротация логов настроена"
echo "  ✅ Systemd сервис создан"
echo
log_info "🔄 Следующие шаги:"
echo "  1. Перезайдите на сервер или выполните: newgrp docker"
echo "  2. Переключитесь на пользователя callchecker: su - callchecker"
echo "  3. Клонируйте репозиторий в $WORK_DIR"
echo "  4. Настройте env.production с вашими ключами"
echo "  5. Запустите: ./deploy_production.sh"
echo
log_info "🔧 Дополнительные команды:"
echo "  Автозапуск при загрузке: systemctl enable callchecker"
echo "  Запуск сервиса:          systemctl start callchecker"
echo "  Статус сервиса:          systemctl status callchecker"
echo "  Мониторинг ресурсов:     htop"
echo "  Проверка дискового пространства: df -h"
echo
log_success "✅ Сервер готов для развертывания Callchecker!"

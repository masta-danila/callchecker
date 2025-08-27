# Multi-stage build для оптимизации размера образа
FROM python:3.12-slim as builder

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Копируем и устанавливаем Python зависимости
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

# Основной образ
FROM python:3.12-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Создание пользователя для безопасности
RUN useradd -m -u 1000 callchecker

# Копирование Python пакетов из builder
COPY --from=builder /root/.local /home/callchecker/.local

# Настройка рабочей директории
WORKDIR /app

# Копирование кода приложения
COPY . .

# Создание необходимых директорий
RUN mkdir -p /app/logs /app/upload /app/bitrix24/downloads

# Настройка прав доступа
RUN chown -R callchecker:callchecker /app

# Переключение на непривилегированного пользователя
USER callchecker

# Добавление локальных пакетов в PATH
ENV PATH="/home/callchecker/.local/bin:$PATH"

# Переменная окружения для Python
ENV PYTHONPATH="/app:$PYTHONPATH"
ENV PYTHONUNBUFFERED=1

# Проверка здоровья контейнера
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Экспорт порта (если понадобится веб-интерфейс)
EXPOSE 8000

# Команда по умолчанию (будет переопределена в docker-compose)
CMD ["python", "dialog_analysis.py"]

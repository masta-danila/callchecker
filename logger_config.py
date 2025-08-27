import logging
import sys
from logging.handlers import RotatingFileHandler
import os


def setup_logger(name: str, log_file: str = None, level: str = "INFO"):
    """
    Настройка логгера с выводом в консоль и файл
    
    Args:
        name: Имя логгера (обычно __name__)
        log_file: Путь к файлу логов (опционально)
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Избегаем дублирования хендлеров
    if logger.handlers:
        return logger
    
    # Формат логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Консольный вывод (для Docker logs)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Файловый вывод (если указан путь)
    if log_file:
        # Создаем директорию для логов
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Ротация логов: максимум 10MB, до 5 файлов
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# Готовые логгеры для ваших модулей
def get_main_logger():
    return setup_logger('bitrix_main', 'logs/bitrix_main.log')

def get_dialogue_logger():
    return setup_logger('dialogue_recognition', 'logs/dialogue_recognition.log')

def get_analysis_logger():
    return setup_logger('dialog_analysis', 'logs/dialog_analysis.log')

def get_sheets_logger():
    return setup_logger('google_sheets', 'logs/google_sheets.log')

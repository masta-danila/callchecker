import json
import os
import re
import sys
from urllib.parse import urlparse

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_create_table import create_tables
from logger_config import setup_logger

# Настройка логгера для этого модуля
logger = setup_logger('setup_portals', 'logs/setup_portals.log')


def load_portals_config(config_file='bitrix_portals.json'):
    """
    Загружает конфигурацию порталов из JSON файла
    """
    try:
        # Определяем путь относительно текущего файла
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, config_file)
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config.get('portals', [])
    except FileNotFoundError:
        logger.error(f"Ошибка: файл конфигурации {config_file} не найден в {current_dir}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка в JSON файле: {e}")
        return []


def extract_portal_name(url):
    """
    Извлекает имя портала из URL
    Например: advertpro из https://advertpro.bitrix24.ru/rest/9/token
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if hostname and '.bitrix24.' in hostname:
            portal_name = hostname.split('.bitrix24.')[0]
            return portal_name
        return None
    except Exception as e:
        logger.error(f"Ошибка при парсинге URL {url}: {e}")
        return None


def validate_portal_url(url):
    """
    Проверяет корректность URL портала
    """
    pattern = r'https://[\w-]+\.bitrix24\.ru/rest/\d+/[\w]+/?$'
    return bool(re.match(pattern, url))


def get_existing_portal_tables():
    """
    Получает список существующих таблиц порталов из базы данных
    """
    try:
        from db_client import get_db_client
        
        with get_db_client() as conn:
            with conn.cursor() as cur:
                # Ищем только основные таблицы порталов (исключаем все служебные таблицы)
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name NOT LIKE '%_entities'
                    AND table_name NOT LIKE '%_users'
                    AND table_name NOT LIKE '%_categories'
                    AND table_name NOT LIKE '%_criteria'
                    AND table_name NOT LIKE '%_criterion_groups'
                    AND table_name NOT LIKE '%_categories_criteria'
                    AND table_type = 'BASE TABLE'
                """)
                
                tables = [row[0] for row in cur.fetchall()]
                return tables
    except Exception as e:
        logger.error(f"Ошибка при получении списка таблиц: {e}")
        return []


def drop_portal_tables(portal_name):
    """
    Удаляет все таблицы для указанного портала
    """
    try:
        from db_client import get_db_client
        
        with get_db_client() as conn:
            with conn.cursor() as cur:
                # Удаляем все связанные таблицы
                tables_to_drop = [
                    f"{portal_name}_categories_criteria",
                    f"{portal_name}_categories", 
                    f"{portal_name}_criteria",
                    f"{portal_name}_criterion_groups",
                    f"{portal_name}",
                    f"{portal_name}_entities"
                ]
                
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.debug(f"Удалена таблица {table}")
                
                logger.info(f"Таблицы для портала {portal_name} удалены")
                return True
    except Exception as e:
        logger.error(f"Ошибка при удалении таблиц для портала {portal_name}: {e}")
        return False


def cleanup_unused_portals(config_portals):
    """
    Удаляет таблицы порталов, которых нет в конфиге
    """
    existing_tables = get_existing_portal_tables()
    
    # Извлекаем имена порталов из конфигурации, поддерживая новый формат
    config_portal_names = []
    for portal in config_portals:
        if isinstance(portal, dict):
            portal_url = portal.get('url', '')
        else:
            portal_url = portal
        
        portal_name = extract_portal_name(portal_url)
        if portal_name:
            config_portal_names.append(portal_name)
    
    unused_portals = [table for table in existing_tables if table not in config_portal_names]
    
    if unused_portals:
        logger.warning(f"Найдены неиспользуемые порталы: {unused_portals}")
        for portal in unused_portals:
            drop_portal_tables(portal)
    else:
        logger.info("Неиспользуемых порталов не найдено")


def portal_tables_exist(portal_name):
    """
    Проверяет, существуют ли таблицы для указанного портала
    """
    try:
        from db_client import get_db_client
        
        with get_db_client() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                """, (portal_name,))
                
                count = cur.fetchone()[0]
                return count > 0
    except Exception as e:
        logger.error(f"Ошибка при проверке существования таблиц для {portal_name}: {e}")
        return False


def setup_portal_tables(portal_config):
    """
    Создает таблицы для одного портала
    """
    logger.info(f"Обрабатываю URL: {portal_config}")
    
    # Извлекаем URL из конфигурации портала
    if isinstance(portal_config, dict):
        portal_url = portal_config.get('url', '')
    else:
        portal_url = portal_config
    
    if not validate_portal_url(portal_url):
        logger.error(f"Некорректный URL: {portal_url}")
        return False
    
    portal_name = extract_portal_name(portal_url)
    if not portal_name:
        logger.error(f"Не удалось извлечь имя портала из URL: {portal_url}")
        return False
    
    logger.info(f"Имя портала: {portal_name}")
    
    # Проверяем, существуют ли уже таблицы
    if portal_tables_exist(portal_name):
        logger.info(f"Таблицы для портала {portal_name} уже существуют, пропускаю")
        return True
    
    try:
        logger.info(f"Создаю таблицы для портала {portal_name}")
        create_tables(portal_name)
        logger.info(f"Таблицы для портала {portal_name} созданы успешно")
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц для портала {portal_name}: {e}")
        return False


def main():
    """
    Основная функция
    """
    logger.info("Настройка таблиц для Bitrix24 порталов")
    
    portals = load_portals_config()
    if not portals:
        logger.warning("Нет порталов для обработки")
        return
    
    logger.info(f"Найдено порталов: {len(portals)}")
    
    # Удаляем таблицы порталов, которых нет в конфиге
    logger.info("Очистка неиспользуемых порталов:")
    cleanup_unused_portals(portals)
    
    success_count = 0
    error_count = 0
    
    for i, portal_config in enumerate(portals, 1):
        logger.info(f"--- Портал {i}/{len(portals)} ---")
        
        if setup_portal_tables(portal_config):
            success_count += 1
        else:
            error_count += 1
    
    logger.info(f"Обработка завершена")
    logger.info(f"Успешно: {success_count}")
    if error_count > 0:
        logger.warning(f"Ошибки: {error_count}")
    else:
        logger.info("Все порталы обработаны без ошибок")


if __name__ == "__main__":
    main()
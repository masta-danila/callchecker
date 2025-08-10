import json
import os
import sys
import re
from urllib.parse import urlparse

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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
        print(f"Ошибка: файл конфигурации {config_file} не найден в {current_dir}")
        return []
    except json.JSONDecodeError as e:
        print(f"Ошибка в JSON файле: {e}")
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
        print(f"Ошибка при парсинге URL {url}: {e}")
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
                # Ищем таблицы, которые не заканчиваются на _entities, _categories и т.д.
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name NOT LIKE '%_entities'
                    AND table_name NOT LIKE '%_categories'
                    AND table_name NOT LIKE '%_criteria'
                    AND table_name NOT LIKE '%_criterion_groups'
                    AND table_name NOT LIKE '%_categories_criteria'
                    AND table_type = 'BASE TABLE'
                """)
                
                tables = [row[0] for row in cur.fetchall()]
                return tables
    except Exception as e:
        print(f"Ошибка при получении списка таблиц: {e}")
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
                
                print(f"Таблицы для портала {portal_name} удалены")
                return True
    except Exception as e:
        print(f"Ошибка при удалении таблиц для портала {portal_name}: {e}")
        return False


def cleanup_unused_portals(config_portals):
    """
    Удаляет таблицы порталов, которых нет в конфиге
    """
    existing_tables = get_existing_portal_tables()
    config_portal_names = [extract_portal_name(url) for url in config_portals]
    config_portal_names = [name for name in config_portal_names if name]
    
    unused_portals = [table for table in existing_tables if table not in config_portal_names]
    
    if unused_portals:
        print(f"Найдены неиспользуемые порталы: {unused_portals}")
        for portal in unused_portals:
            drop_portal_tables(portal)
    else:
        print("Неиспользуемых порталов не найдено")


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
        print(f"Ошибка при проверке существования таблиц для {portal_name}: {e}")
        return False


def setup_portal_tables(portal_url):
    """
    Создает таблицы для одного портала
    """
    print(f"Обрабатываю URL: {portal_url}")
    
    if not validate_portal_url(portal_url):
        print(f"Некорректный URL: {portal_url}")
        return False
    
    portal_name = extract_portal_name(portal_url)
    if not portal_name:
        print(f"Не удалось извлечь имя портала из URL: {portal_url}")
        return False
    
    print(f"Имя портала: {portal_name}")
    
    # Проверяем, существуют ли уже таблицы
    if portal_tables_exist(portal_name):
        print(f"Таблицы для портала {portal_name} уже существуют, пропускаю")
        return True
    
    try:
        from db_create_table import create_tables
        print(f"Создаю таблицы для портала {portal_name}")
        create_tables(portal_name)
        print(f"Таблицы для портала {portal_name} созданы успешно")
        return True
    except Exception as e:
        print(f"Ошибка при создании таблиц для портала {portal_name}: {e}")
        return False


def main():
    """
    Основная функция
    """
    print("Настройка таблиц для Bitrix24 порталов")
    
    portals = load_portals_config()
    if not portals:
        print("Нет порталов для обработки")
        return
    
    print(f"Найдено порталов: {len(portals)}")
    
    # Удаляем таблицы порталов, которых нет в конфиге
    print("\nОчистка неиспользуемых порталов:")
    cleanup_unused_portals(portals)
    
    success_count = 0
    error_count = 0
    
    for i, portal_url in enumerate(portals, 1):
        print(f"\n--- Портал {i}/{len(portals)} ---")
        
        if setup_portal_tables(portal_url):
            success_count += 1
        else:
            error_count += 1
    
    print(f"\nОбработка завершена")
    print(f"Успешно: {success_count}")
    print(f"Ошибки: {error_count}")


if __name__ == "__main__":
    main()
import asyncio
import json
import os
import sys
import aiohttp
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Добавляем корневую папку в путь для импорта модулей  
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from call_downloader import download_call_by_id

# Утилиты для работы с префиксами
def add_call_prefix(call_id):
    """Добавляет префикс call_ к ID если его нет"""
    if isinstance(call_id, str) and call_id.startswith('call_'):
        return call_id
    return f'call_{call_id}'

def remove_call_prefix(call_id):
    """Убирает префикс call_ из ID"""
    if isinstance(call_id, str) and call_id.startswith('call_'):
        return call_id.replace('call_', '')
    return call_id


def load_portals_config(config_file='bitrix_portals.json'):
    """
    Загружает конфигурацию порталов из JSON файла.
    """
    try:
        # Определяем путь относительно текущего файла
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, config_file)
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Ошибка: файл конфигурации {config_file} не найден в {current_dir}")
        return {'portals': [], 'entityTypes': {}}
    except json.JSONDecodeError as e:
        print(f"Ошибка в JSON файле: {e}")
        return {'portals': [], 'entityTypes': {}}


def get_entity_type_id(entity_type, config=None):
    """
    Получает ID типа сущности из конфигурации.
    
    :param entity_type: Тип сущности ('LEAD', 'DEAL', 'CONTACT', 'COMPANY')
    :param config: Конфигурация порталов (если None - загружается автоматически)
    :return: ID типа сущности
    """
    if config is None:
        config = load_portals_config()
    
    entity_types = config.get('entityTypes', {})
    return entity_types.get(entity_type)


def extract_portal_info(portal_url):
    """
    Извлекает имя портала, user_id и токен из ссылки.
    """
    if portal_url.startswith('@'):
        portal_url = portal_url[1:]
    
    try:
        parsed = urlparse(portal_url)
        hostname = parsed.hostname
        
        if hostname and '.bitrix24.' in hostname:
            portal_name = hostname.split('.bitrix24.')[0]
            path_parts = parsed.path.strip('/').split('/')
            if len(path_parts) >= 3 and path_parts[0] == 'rest':
                user_id = path_parts[1]
                token = path_parts[2]
                return portal_name, user_id, token
        
        return None, None, None
    except Exception:
        return None, None, None




async def download_call_with_retry(portal_name, user_id, token, call_id, retries, request_delay):
    """
    Скачивает звонок с повторными попытками.
    """
    for attempt in range(retries):
        try:
            result = await download_call_by_id(portal_name, user_id, token, call_id)
            if result:
                return result
            
            if attempt < retries - 1:
                await asyncio.sleep(request_delay)
                
        except Exception as e:
            print(f"Попытка {attempt + 1} для звонка {call_id}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(request_delay)
    
    return None



async def download_missing_calls_from_records(
    records,
    max_concurrent_requests=50,
    request_delay=0.1,
    retries=3
):
    """
    Скачивает недостающие файлы на основе records из БД.
    Получает звонки из Bitrix24 за период и исключает уже имеющиеся в БД.
    Период поиска (days_back) настраивается индивидуально для каждого портала в bitrix_portals.json.
    
    :param records: Словарь структуры {portal_name: {"records": [{"id": "123"}, ...]}}
    :param max_concurrent_requests: Максимальное количество одновременных запросов
    :param request_delay: Задержка между запросами в секундах  
    :param retries: Количество повторных попыток при ошибке
    :return: Словарь той же структуры с ID успешно скачанных файлов
    """
    
    # Загружаем конфигурацию порталов
    config = load_portals_config()
    portals = config.get('portals', [])
    if not portals:
        print("Нет порталов в конфигурации")
        return {}
    
    # Создаем маппинг portal_name -> (portal_url, user_id, token, days_back)
    portal_urls = {}
    default_days_back = config.get('default_settings', {}).get('days_back', 7)
    
    for portal_config in portals:
        # Поддерживаем старый формат (строка) и новый (объект)
        if isinstance(portal_config, str):
            portal_url = portal_config
            portal_days_back = default_days_back
        else:
            portal_url = portal_config.get('url', '')
            portal_days_back = portal_config.get('days_back', default_days_back)
        
        portal_name, user_id, token = extract_portal_info(portal_url)
        if portal_name:
            portal_urls[portal_name] = (portal_url, user_id, token, portal_days_back)
    
    downloaded_records = {}
    
    # Получаем список всех порталов для обработки
    all_portals_to_process = set()
    
    # Добавляем порталы из records
    all_portals_to_process.update(records.keys())
    
    # Добавляем все порталы из конфига (на случай если база пуста)
    all_portals_to_process.update(portal_urls.keys())
    
    # Обрабатываем каждый портал
    for portal_name in all_portals_to_process:
        if portal_name not in portal_urls:
            print(f"Портал {portal_name} не найден в конфигурации, пропускаю")
            continue
            
        portal_url, user_id, token, portal_days_back = portal_urls[portal_name]
        
        # Получаем данные портала из records (если есть)
        portal_data = records.get(portal_name, {"records": []})
        
        # Обрабатываем ID из БД - они могут быть с префиксами (call_493125) или без (493125)
        # Для сравнения с Bitrix24 API нужны оригинальные ID без префиксов
        call_ids = []
        prefixed_ids = []
        for record in portal_data.get("records", []):
            record_id = record["id"]
            prefixed_ids.append(add_call_prefix(record_id))  # Сохраняем с префиксом для внутренней работы
            call_ids.append(remove_call_prefix(record_id))   # Без префикса для сравнения с API
        
        print(f"Обрабатываю портал {portal_name}: {len(prefixed_ids)} записей в БД, {len(call_ids)} ID для сравнения с API")
        
        if not call_ids:
            print(f"В БД нет записей для портала {portal_name}, ищу все звонки за период")
        else:
            print(f"Обрабатываю {len(call_ids)} записей для портала {portal_name}")
        
        # Скачиваем файлы для этого портала (исключая уже имеющиеся в БД)
        print(f"Использую days_back={portal_days_back} для портала {portal_name}")
        successful_downloads = await download_calls_for_portal(
            portal_name, user_id, token, call_ids, 
            max_concurrent_requests, request_delay, retries, portal_days_back
        )
        
        # Формируем результат с полной информацией о звонках
        if successful_downloads:
            # Добавляем префиксы к ID перед сохранением в результат
            records_with_prefixes = []
            for record in successful_downloads:
                record_copy = record.copy()
                record_copy['id'] = add_call_prefix(record['id'])
                records_with_prefixes.append(record_copy)
            
            downloaded_records[portal_name] = {
                "records": records_with_prefixes
            }
    
    return downloaded_records


async def get_portal_calls(portal_name, user_id, token, days_back=7):
    """
    Получает все звонки с записями из Bitrix24 за указанный период.
    Возвращает полную информацию о звонках.
    """
    # Исправленная логика: days_back=1 означает сегодня, days_back=2 означает сегодня+вчера
    days_to_subtract = days_back - 1
    start_date = (datetime.now() - timedelta(days=days_to_subtract)).strftime('%Y-%m-%d')
    url = f"https://{portal_name}.bitrix24.ru/rest/{user_id}/{token}/voximplant.statistic.get.json"
    
    all_calls = []
    start = 0
    limit = 50
    
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                params = {
                    'FILTER[>CALL_START_DATE]': start_date,
                    'order[CALL_START_DATE]': 'DESC',
                    'start': start,
                    'limit': limit
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if 'result' in data and len(data['result']) > 0:
                            # Фильтруем только звонки с записями и извлекаем полную информацию
                            for call in data['result']:
                                if call.get('CALL_RECORD_URL'):
                                    call_info = {
                                        'id': call['ID'],
                                        'date': call.get('CALL_START_DATE'),
                                        'user_id': call.get('PORTAL_USER_ID'),
                                        'phone_number': call.get('PHONE_NUMBER'),
                                        'entity_id': call.get('CRM_ENTITY_ID', 1),  # Если нет - ставим 1 по умолчанию
                                        'call_type': call.get('CALL_TYPE'),  # 1-исходящий, 2-входящий, 3-перенаправление, 4-обратный
                                        'crm_entity_type': call.get('CRM_ENTITY_TYPE')  # Тип объекта CRM (LEAD, CONTACT, COMPANY)
                                    }
                                    all_calls.append(call_info)
                            
                            # Проверяем, есть ли еще данные
                            if data.get('next'):
                                start = data['next']
                            else:
                                break
                        else:
                            break
                    else:
                        print(f"Ошибка API для портала {portal_name}: {response.status}")
                        break
                        
    except Exception as e:
        print(f"Ошибка при получении звонков для портала {portal_name}: {e}")
    
    return all_calls


async def download_calls_for_portal(portal_name, user_id, token, existing_call_ids, max_concurrent_requests, request_delay, retries, days_back=7):
    """
    Получает звонки из Bitrix24, исключает уже имеющиеся в БД и скачивает новые.
    """
    # Получаем все звонки из Bitrix24
    all_bitrix_calls = await get_portal_calls(portal_name, user_id, token, days_back)
    
    if not all_bitrix_calls:
        print(f"Нет звонков с записями в Bitrix24 для портала {portal_name}")
        return []
    
    # Исключаем те, что уже есть в БД
    existing_ids_set = set(existing_call_ids)
    new_calls = [call for call in all_bitrix_calls if call['id'] not in existing_ids_set]
    
    # Проверяем какие файлы уже скачаны в папке
    # Путь к папке downloads относительно текущего файла
    current_dir = os.path.dirname(os.path.abspath(__file__))
    downloads_dir = os.path.join(current_dir, "downloads", portal_name)
    downloaded_files = set()
    
    if os.path.exists(downloads_dir):
        for filename in os.listdir(downloads_dir):
            if filename.endswith('.mp3'):
                # Извлекаем ID из имени файла
                # Поддерживаем как старые (493123.mp3) так и новые (call_493123.mp3) форматы
                call_id = filename.replace('.mp3', '')
                
                # Приводим к формату без префикса для сравнения с API
                original_id = remove_call_prefix(call_id)
                downloaded_files.add(original_id)
    
    # Исключаем те, что уже скачаны в папке (только для скачивания)
    final_calls = [call for call in new_calls if call['id'] not in downloaded_files]
    
    print(f"Найдено {len(all_bitrix_calls)} звонков в Bitrix24 для портала {portal_name}")
    print(f"Исключено {len(existing_call_ids)} уже имеющихся в БД")
    print(f"Исключено {len(downloaded_files)} уже скачанных файлов")
    print(f"Нужно скачать {len(final_calls)} новых звонков")
    
    # Формируем данные для уже скачанных файлов из папки
    already_downloaded = []
    for call in new_calls:
        if call['id'] in downloaded_files:
            already_downloaded.append(call)
    
    # Скачиваем только недостающие файлы
    newly_downloaded = []
    if final_calls:
        # Создаем семафор для ограничения количества одновременных запросов
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        async def download_with_semaphore(call_info):
            async with semaphore:
                await asyncio.sleep(request_delay)
                result = await download_call_with_retry(portal_name, user_id, token, call_info['id'], retries, request_delay)
                return call_info if result else None
        
        # Запускаем скачивание только тех, что не скачаны
        tasks = [download_with_semaphore(call) for call in final_calls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Собираем информацию об успешно скачанных файлах
        newly_downloaded = [result for result in results if result and not isinstance(result, Exception)]
    
    # В результат включаем ВСЕ доступные файлы с полной информацией
    all_available_calls = already_downloaded + newly_downloaded
    
    print(f"Успешно скачано {len(newly_downloaded)}/{len(final_calls)} новых файлов для портала {portal_name}")
    print(f"Всего доступно файлов: {len(all_available_calls)} (включая ранее скачанные)")
    
    return all_available_calls


if __name__ == "__main__":
    from debug_utils import save_debug_json
    # Тестовые данные - записи с префиксами call_
    test_records = {
        "advertpro": {
            "records": [
                {"id": "call_493213"},
                {"id": "call_493217"}, 
                {"id": "call_493155"},
                {"id": "call_493227"},
                {"id": "call_493205"},
                {"id": "call_493199"},
                {"id": "call_493209"},
                {"id": "call_493143"},
                {"id": "call_493215"},
                {"id": "call_493201"},
                {"id": "call_493211"},
                {"id": "call_493195"},
                {"id": "call_493197"},
                {"id": "call_493183"},
                {"id": "call_493193"},
                {"id": "call_493177"},
                {"id": "call_493191"},
                {"id": "call_493189"},
                {"id": "call_493185"},
                {"id": "call_493175"}
            ]
        }
    }
    test_records = {
    "advertpro": {
        "records": []
    }
    }
    
    async def test():
        result = await download_missing_calls_from_records(
            records=test_records,
            max_concurrent_requests=50,
            request_delay=0.2,
            retries=2
        )
        
        save_debug_json(result, "test_records")
    
    asyncio.run(test())
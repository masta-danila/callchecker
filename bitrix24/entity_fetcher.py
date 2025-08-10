import asyncio
import json
import sys
import os

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitrix_entity_fetcher import fetch_multiple_entities
from debug_utils import save_debug_json


def load_portals_config():
    """
    Загружает конфигурацию порталов из bitrix_portals.json
    """
    try:
        # Определяем путь относительно текущего файла
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, 'bitrix_portals.json')
        
        with open(config_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"Ошибка при загрузке конфигурации порталов: {e}")
        return {}


def extract_portal_info(portal_url):
    """
    Извлекает название портала, user_id и token из URL
    """
    try:
        # Пример URL: https://advertpro.bitrix24.ru/rest/9/nhrww3tccn7ep547
        parts = portal_url.split('/')
        portal_name = parts[2].split('.')[0]  # advertpro
        user_id = parts[4]  # 9
        token = parts[5]  # nhrww3tccn7ep547
        return portal_name, user_id, token
    except Exception as e:
        print(f"Ошибка при парсинге URL портала: {e}")
        return None, None, None


def get_entity_type_id(entity_type, config):
    """
    Получает entity_type_id из конфигурации по названию типа сущности
    """
    entity_types = config.get('entityTypes', {})
    return entity_types.get(entity_type)


async def fetch_entities_for_records(
    records: dict,
    max_concurrent_requests: int = 50,
    request_delay: float = 0.1,
    retries: int = 3
) -> dict:
    """
    Обрабатывает записи звонков, получает данные о сущностях CRM и добавляет их в результат.
    
    :param records: Словарь записей из Шага 2
    :param max_concurrent_requests: Максимальное количество одновременных запросов
    :param request_delay: Задержка между запросами
    :param retries: Количество повторных попыток
    :return: Словарь с добавленными данными сущностей
    """
    print("Шаг 3: Получаю данные сущностей из Bitrix24")
    
    # Загружаем конфигурацию порталов
    config = load_portals_config()
    portals = config.get('portals', [])
    
    if not portals:
        print("Нет порталов в конфигурации")
        return records
    
    # Создаем маппинг portal_name -> portal_info
    portal_info_map = {}
    for portal_config in portals:
        # Поддерживаем старый формат (строка) и новый (объект)
        if isinstance(portal_config, str):
            portal_url = portal_config
        else:
            portal_url = portal_config.get('url', '')
        
        portal_name, user_id, token = extract_portal_info(portal_url)
        if portal_name:
            portal_info_map[portal_name] = (user_id, token)
    
    result_records = {}
    
    # Обрабатываем каждый портал
    for portal_name, portal_data in records.items():
        print(f"Обрабатываю сущности для портала {portal_name}")
        
        if portal_name not in portal_info_map:
            print(f"Портал {portal_name} не найден в конфигурации, пропускаю")
            result_records[portal_name] = portal_data
            continue
        
        user_id, token = portal_info_map[portal_name]
        call_records = portal_data.get("records", [])
        
        # Собираем уникальные сущности для загрузки
        entities_to_fetch = set()
        for record in call_records:
            entity_id = record.get('entity_id')
            crm_entity_type = record.get('crm_entity_type')
            
            # Пропускаем записи без сущности
            if not entity_id or not crm_entity_type:
                continue
                
            entity_type_id = get_entity_type_id(crm_entity_type, config)
            if entity_type_id:
                entities_to_fetch.add((entity_type_id, int(entity_id)))
        
        print(f"Найдено {len(entities_to_fetch)} уникальных сущностей для загрузки")
        
        # Получаем данные сущностей
        entities_data = {}
        if entities_to_fetch:
            entities_data = await fetch_multiple_entities(
                portal_name, user_id, token, list(entities_to_fetch)
            )
            print(f"Успешно загружено данных о {len(entities_data)} сущностях")
        
        # Формируем список сущностей для этого портала
        portal_entities = []
        for entity_key, entity_info in entities_data.items():
            entity_type_id, entity_id = entity_key
            portal_entities.append({
                'entity_type_id': entity_type_id,
                'entity_id': entity_id,
                'title': entity_info.get('title'),
                'name': entity_info.get('name'),
                'lastName': entity_info.get('lastName')
            })
        
        result_records[portal_name] = {
            "records": call_records,  # Оригинальные записи без изменений
            "entities": portal_entities  # Список сущностей
        }
    
    return result_records


if __name__ == "__main__":
    async def test():
        # Жестко заданные тестовые данные (первые 5 записей)
        test_records = {
            "advertpro": {
                "records": [
                    {
                        "id": "493123",
                        "date": "2025-08-07T10:00:19+03:00",
                        "user_id": "11009",
                        "phone_number": "+79189616367",
                        "entity_id": "27523",
                        "call_type": "1",
                        "crm_entity_type": "CONTACT"
                    },
                    {
                        "id": "493125",
                        "date": "2025-08-07T10:01:19+03:00",
                        "user_id": "15437",
                        "phone_number": "+79309620098",
                        "entity_id": "27533",
                        "call_type": "1",
                        "crm_entity_type": "CONTACT"
                    },
                    {
                        "id": "493129",
                        "date": "2025-08-07T10:29:33+03:00",
                        "user_id": "11009",
                        "phone_number": "+78612583661",
                        "entity_id": "347831",
                        "call_type": "1",
                        "crm_entity_type": "LEAD"
                    },
                    {
                        "id": "493135",
                        "date": "2025-08-07T10:32:19+03:00",
                        "user_id": "11009",
                        "phone_number": "+79054856753",
                        "entity_id": "347833",
                        "call_type": "1",
                        "crm_entity_type": "LEAD"
                    },
                    {
                        "id": "493211",
                        "date": "2025-08-07T13:34:23+03:00",
                        "user_id": "13961",
                        "phone_number": "+79313514929",
                        "entity_id": None,
                        "call_type": "1",
                        "crm_entity_type": None
                    }
                ]
            }
        }
        
        print(f"Тестирую на {len(test_records['advertpro']['records'])} записях")
        
        # Запускаем получение данных сущностей
        result = await fetch_entities_for_records(
            records=test_records,
            max_concurrent_requests=10,
            request_delay=0.2,
            retries=2
        )
        
        # Сохраняем результат для отладки
        save_debug_json(result, "test_entity_fetching")
        
        print("Тестирование завершено, результат сохранен в bitrix24/json_tests/test_entity_fetching.json")
    
    asyncio.run(test())
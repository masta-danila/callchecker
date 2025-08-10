import asyncio
import aiohttp
import json
import os
import sys

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


async def fetch_users_from_bitrix(portal_name, user_id, token, filters=None, sort_field="ID", sort_order="ASC", start=0):
    """
    Получает список пользователей из Bitrix24 асинхронно.
    
    :param portal_name: Название портала (например, 'advertpro')
    :param user_id: ID пользователя
    :param token: Токен доступа
    :param filters: Словарь фильтров (например, {'ACTIVE': True, 'UF_DEPARTMENT': 1})
    :param sort_field: Поле для сортировки (по умолчанию "ID")
    :param sort_order: Направление сортировки "ASC" или "DESC"
    :param start: Начальная позиция для пагинации (по умолчанию 0)
    :return: Список пользователей или None при ошибке
    """
    url = f"https://{portal_name}.bitrix24.ru/rest/{user_id}/{token}/user.get"
    
    # Формируем тело запроса
    payload = {
        "SORT": sort_field,
        "ORDER": sort_order,
        "start": start
    }
    
    # Добавляем фильтры если они указаны
    if filters:
        for key, value in filters.items():
            payload[key] = value
    
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'result' in data:
                        users = data['result']
                        next_page = data.get('next')
                        
                        return {
                            'users': users,
                            'next': next_page,
                            'total': data.get('total', len(users))
                        }
                    else:
                        print(f"Нет данных в ответе для пользователей")
                        return None
                else:
                    print(f"Ошибка API: {response.status}")
                    response_text = await response.text()
                    print(f"Ответ сервера: {response_text}")
                    return None
                    
    except Exception as e:
        print(f"Ошибка при получении пользователей: {e}")
        return None


async def fetch_all_users_from_bitrix(portal_name, user_id, token, filters=None, sort_field="ID", sort_order="ASC"):
    """
    Получает всех пользователей из Bitrix24 с автоматической пагинацией.
    
    :param portal_name: Название портала
    :param user_id: ID пользователя
    :param token: Токен доступа
    :param filters: Словарь фильтров
    :param sort_field: Поле для сортировки
    :param sort_order: Направление сортировки
    :return: Список всех пользователей
    """
    all_users = []
    start = 0
    
    print(f"Получаю пользователей из портала {portal_name}")
    
    while True:
        print(f"Запрашиваю пользователей начиная с позиции {start}")
        
        result = await fetch_users_from_bitrix(
            portal_name, user_id, token, filters, sort_field, sort_order, start
        )
        
        if not result:
            print("Ошибка при получении пользователей, прерываю")
            break
            
        users = result.get('users', [])
        if not users:
            print("Больше пользователей нет")
            break
            
        all_users.extend(users)
        print(f"Получено {len(users)} пользователей, всего: {len(all_users)}")
        
        # Проверяем есть ли следующая страница
        next_page = result.get('next')
        if not next_page:
            print("Достигнут конец списка пользователей")
            break
            
        start = next_page
    
    print(f"Получено всего {len(all_users)} пользователей")
    return all_users


async def fetch_active_users_from_bitrix(portal_name, user_id, token, department_id=None):
    """
    Получает список только активных пользователей из Bitrix24.
    
    :param portal_name: Название портала
    :param user_id: ID пользователя
    :param token: Токен доступа
    :param department_id: ID отдела (опционально)
    :return: Список активных пользователей
    """
    # Фильтр для активных пользователей
    filters = {
        'ACTIVE': True,
        'USER_TYPE': 'employee'  # Только сотрудники, исключаем экстранет и почтовых
    }
    
    # Добавляем фильтр по отделу если указан
    if department_id:
        filters['UF_DEPARTMENT'] = department_id
    
    print(f"Получаю активных сотрудников" + (f" из отдела {department_id}" if department_id else ""))
    
    return await fetch_all_users_from_bitrix(
        portal_name, user_id, token, 
        filters=filters,
        sort_field="LAST_NAME",  # Сортируем по фамилии
        sort_order="ASC"
    )


async def fetch_users_for_portal(portal_url, filters=None):
    """
    Получает пользователей для портала по URL.
    
    :param portal_url: URL портала (например, "https://advertpro.bitrix24.ru/rest/9/token123")
    :param filters: Дополнительные фильтры
    :return: Словарь с пользователями {portal_name: {"users": [...]}}
    """
    # Извлекаем данные из URL
    try:
        parts = portal_url.split('/')
        portal_name = parts[2].split('.')[0]  # advertpro
        user_id = parts[4]  # 9
        token = parts[5]  # token123
    except Exception as e:
        print(f"Ошибка при парсинге URL портала: {e}")
        return {}
    
    print(f"Получаю пользователей для портала {portal_name}")
    
    if filters is None:
        # По умолчанию получаем только активных сотрудников
        users_list = await fetch_active_users_from_bitrix(portal_name, user_id, token)
    else:
        users_list = await fetch_all_users_from_bitrix(portal_name, user_id, token, filters)
    
    # Возвращаем в формате словаря
    return {
        portal_name: {
            "users": users_list if users_list else []
        }
    }


async def fetch_users_from_all_portals(filters=None, max_concurrent_portals=3, request_delay=0.1, retries=3):
    """
    Получает пользователей из всех порталов, указанных в конфигурации.
    
    :param filters: Дополнительные фильтры для пользователей
    :param max_concurrent_portals: Максимальное количество одновременных запросов к порталам
    :param request_delay: Задержка между запросами в секундах
    :param retries: Количество повторных попыток при ошибке
    :return: Словарь {portal_name: {"users": [...]}} со всеми пользователями
    """
    print("Получаю пользователей из всех порталов")
    
    # Загружаем конфигурацию порталов
    config = load_portals_config()
    portals = config.get('portals', [])
    
    if not portals:
        print("Нет порталов в конфигурации")
        return {}
    
    print(f"Найдено {len(portals)} порталов, максимум одновременных запросов: {max_concurrent_portals}")
    
    # Создаем семафор для ограничения количества одновременных запросов
    semaphore = asyncio.Semaphore(max_concurrent_portals)
    
    async def fetch_portal_with_semaphore(portal_url):
        """Получает пользователей портала с ограничением семафора и повторными попытками"""
        async with semaphore:
            print(f"Обрабатываю портал: {portal_url}")
            
            for attempt in range(retries + 1):
                try:
                    # Добавляем задержку между запросами
                    if request_delay > 0:
                        await asyncio.sleep(request_delay)
                    
                    result = await fetch_users_for_portal(portal_url, filters)
                    if result:  # Успешный результат
                        if attempt > 0:
                            print(f"Успешно получены пользователи из {portal_url} с попытки {attempt + 1}")
                        return result
                    else:
                        raise Exception("Пустой результат от портала")
                        
                except Exception as e:
                    if attempt < retries:
                        retry_delay = request_delay * (2 ** attempt)  # Экспоненциальная задержка
                        print(f"Ошибка при получении пользователей из {portal_url} (попытка {attempt + 1}/{retries + 1}): {e}")
                        print(f"Повторная попытка через {retry_delay:.1f} секунд...")
                        await asyncio.sleep(retry_delay)
                    else:
                        print(f"Не удалось получить пользователей из {portal_url} после {retries + 1} попыток: {e}")
                        return {}
    
    # Создаем задачи для всех порталов
    portal_urls = []
    for portal_config in portals:
        # Поддерживаем старый формат (строка) и новый (объект)
        if isinstance(portal_config, str):
            portal_urls.append(portal_config)
        else:
            portal_urls.append(portal_config.get('url', ''))
    
    tasks = [fetch_portal_with_semaphore(portal_url) for portal_url in portal_urls]
    
    # Выполняем все задачи параллельно
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Собираем результаты
    all_users = {}
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Ошибка при обработке портала {portals[i]}: {result}")
        elif isinstance(result, dict):
            all_users.update(result)
    
    # Подсчет общего количества пользователей
    total_users = sum(len(portal_data.get("users", [])) for portal_data in all_users.values())
    print(f"Получено всего {total_users} пользователей из {len(all_users)} порталов")
    
    return all_users


async def add_users_to_records(records_dict, max_concurrent_portals=3, request_delay=0.1, retries=3):
    """
    Добавляет данные пользователей к записям звонков.
    Принимает результат после Шага 4, извлекает user_id из записей,
    получает данные пользователей из Bitrix24 и добавляет ключ 'users' в словарь.
    
    :param records_dict: Словарь после Шага 4 с записями и сущностями
    :param max_concurrent_portals: Максимальное количество одновременных запросов к порталам
    :param request_delay: Задержка между запросами в секундах
    :param retries: Количество повторных попыток при ошибке
    :return: Обогащенный словарь с добавленным ключом 'users'
    """
    print("Обогащаю записи данными пользователей")
    
    # Загружаем конфигурацию порталов
    config = load_portals_config()
    portals = config.get('portals', [])
    
    if not portals:
        print("Нет порталов в конфигурации")
        return records_dict
    
    # Создаем маппинг portal_name -> portal_info
    portal_info_map = {}
    for portal_config in portals:
        try:
            # Поддерживаем старый формат (строка) и новый (объект)
            if isinstance(portal_config, str):
                portal_url = portal_config
            else:
                portal_url = portal_config.get('url', '')
            
            parts = portal_url.split('/')
            portal_name = parts[2].split('.')[0]  # advertpro
            user_id = parts[4]  # 9
            token = parts[5]  # token123
            portal_info_map[portal_name] = (user_id, token)
        except Exception as e:
            print(f"Ошибка при парсинге URL портала {portal_config}: {e}")
    
    result_dict = records_dict.copy()
    
    # Обрабатываем каждый портал
    for portal_name, portal_data in records_dict.items():
        if portal_name not in portal_info_map:
            print(f"Портал {portal_name} не найден в конфигурации, пропускаю")
            result_dict[portal_name]['users'] = []
            continue
        
        records = portal_data.get('records', [])
        
        # Проверяем, есть ли записи вообще
        if not records:
            print(f"Нет записей для портала {portal_name}, пропускаю запросы к API")
            result_dict[portal_name]['users'] = []
            continue
        
        user_id, token = portal_info_map[portal_name]
        
        # Собираем уникальные user_id из записей
        user_ids = set()
        for record in records:
            record_user_id = record.get('user_id')
            if record_user_id:
                user_ids.add(str(record_user_id))  # Приводим к строке для единообразия
        
        if not user_ids:
            print(f"Нет user_id в записях для портала {portal_name}, пропускаю запросы к API")
            result_dict[portal_name]['users'] = []
            continue
        
        print(f"Найдено {len(user_ids)} уникальных пользователей в записях портала {portal_name}")
        
        # Получаем всех активных пользователей портала
        try:
            all_portal_users = await fetch_active_users_from_bitrix(portal_name, user_id, token)
            
            if not all_portal_users:
                print(f"Не удалось получить пользователей для портала {portal_name}")
                result_dict[portal_name]['users'] = []
                continue
            
            # Фильтруем только нужных пользователей и извлекаем нужные поля
            filtered_users = []
            for user in all_portal_users:
                if str(user.get('ID', '')) in user_ids:
                    user_data = {
                        'id': user.get('ID'),
                        'NAME': user.get('NAME'),
                        'LAST_NAME': user.get('LAST_NAME'),
                        'UF_DEPARTMENT': user.get('UF_DEPARTMENT')
                    }
                    filtered_users.append(user_data)
            
            result_dict[portal_name]['users'] = filtered_users
            print(f"Добавлено {len(filtered_users)} пользователей для портала {portal_name}")
            
        except Exception as e:
            print(f"Ошибка при получении пользователей для портала {portal_name}: {e}")
            result_dict[portal_name]['users'] = []
    
    return result_dict


if __name__ == "__main__":
    async def test():
        print("Тестирую обогащение записей данными пользователей")
        
        # Жестко заданные тестовые данные (как после Шага 4)
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
                        "user_id": "11009",  # Повторяющийся user_id для проверки уникальности
                        "phone_number": "+78612583661",
                        "entity_id": "347831",
                        "call_type": "1",
                        "crm_entity_type": "LEAD"
                    }
                ],
                "entities": [
                    {
                        "id": 15,
                        "entity_type_id": 3,
                        "entity_id": 27523,
                        "title": "Тестовый контакт",
                        "name": "Иван",
                        "lastName": "Петров"
                    }
                ]
            },
            "empty_portal": {
                "records": [],  # Пустые записи для проверки оптимизации
                "entities": []
            },
            "no_users_portal": {
                "records": [
                    {
                        "id": "123456",
                        "date": "2025-08-07T10:00:19+03:00",
                        "user_id": None,  # Нет user_id
                        "phone_number": "+79999999999"
                    }
                ],
                "entities": []
            }
        }
        
        print("Используются жестко заданные тестовые данные")
        print("Проверяю оптимизацию: пропуск пустых записей и записей без user_id")
        
        # Тестируем функцию обогащения
        enriched_records = await add_users_to_records(test_records)
        
        # Сохраняем результат
        save_debug_json(enriched_records, "enriched_with_users_test")
        print("\nРезультат сохранен в json_tests/enriched_with_users_test.json")
    
    asyncio.run(test())
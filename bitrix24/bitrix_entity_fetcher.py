import asyncio
import aiohttp
import json


async def fetch_entity_from_bitrix(portal_name, user_id, token, entity_type_id, entity_id):
    """
    Получает данные сущности из Bitrix24 CRM асинхронно.
    
    :param portal_name: Название портала (например, 'advertpro')
    :param user_id: ID пользователя
    :param token: Токен доступа
    :param entity_type_id: Тип сущности (1=LEAD, 2=DEAL, 3=CONTACT, 4=COMPANY)
    :param entity_id: ID сущности
    :return: Словарь с данными сущности или None при ошибке
    """
    url = f"https://{portal_name}.bitrix24.ru/rest/{user_id}/{token}/crm.item.get"
    
    payload = {
        "entityTypeId": entity_type_id,
        "id": entity_id,
        "useOriginalUfNames": "N"
    }
    
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'result' in data and 'item' in data['result']:
                        item = data['result']['item']
                        
                        # Извлекаем нужные поля
                        entity_data = {
                            'title': item.get('title'),
                            'name': item.get('name'),
                            'lastName': item.get('lastName')
                        }
                        
                        return entity_data
                    else:
                        print(f"Нет данных в ответе для сущности {entity_type_id}:{entity_id}")
                        return None
                else:
                    print(f"Ошибка API: {response.status}")
                    response_text = await response.text()
                    print(f"Ответ сервера: {response_text}")
                    return None
                    
    except Exception as e:
        print(f"Ошибка при получении сущности {entity_type_id}:{entity_id}: {e}")
        return None


async def fetch_multiple_entities(portal_name, user_id, token, entities_list):
    """
    Получает данные нескольких сущностей параллельно.
    
    :param portal_name: Название портала
    :param user_id: ID пользователя
    :param token: Токен доступа
    :param entities_list: Список кортежей (entity_type_id, entity_id)
    :return: Словарь {(entity_type_id, entity_id): entity_data}
    """
    tasks = []
    for entity_type_id, entity_id in entities_list:
        task = fetch_entity_from_bitrix(portal_name, user_id, token, entity_type_id, entity_id)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    entities_data = {}
    for i, result in enumerate(results):
        entity_key = entities_list[i]
        if result and not isinstance(result, Exception):
            entities_data[entity_key] = result
        else:
            print(f"Не удалось получить данные для сущности {entity_key}")
    
    return entities_data


if __name__ == "__main__":
    async def test():
        # Данные для тестирования
        portal_name = "advertpro"
        user_id = "9"
        token = "nhrww3tccn7ep547"
        
        # Тестируем получение сущности
        entity_type_id = 1  # CONTACT
        entity_id = 347843
        
        print("Тестирую получение сущности")
        print(f"Портал: {portal_name}")
        print(f"Тип сущности: {entity_type_id} (CONTACT)")
        print(f"ID сущности: {entity_id}")
        print("-" * 50)
        
        result = await fetch_entity_from_bitrix(
            portal_name, user_id, token, entity_type_id, entity_id
        )
        
        if result:
            print("Результат:")
            print(result)
        else:
            print("Не удалось получить данные сущности")
    
    asyncio.run(test())
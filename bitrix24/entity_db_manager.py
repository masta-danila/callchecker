import asyncio
import sys
import os

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_db_client
from debug_utils import save_debug_json
from logger_config import setup_logger

logger = setup_logger('entity_db_manager', 'logs/entity_db_manager.log')


def upsert_entities_to_db(portal_name: str, entities: list) -> dict:
    """
    Создает или обновляет сущности в таблице {portal_name}_entities и возвращает их внутренние ID.
    
    :param portal_name: Название портала (например, 'advertpro')
    :param entities: Список сущностей с полями entity_type_id, entity_id, title, name, lastName
    :return: Словарь {(entity_type_id, entity_id): internal_id}
    """
    if not entities:
        logger.info(f"Нет сущностей для обновления в портале {portal_name}")
        return {}
    
    logger.info(f"Обновляю {len(entities)} сущностей в БД для портала {portal_name}")
    
    entity_id_mapping = {}
    
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                for entity in entities:
                    entity_type_id = entity.get('entity_type_id')
                    entity_id_bitrix = entity.get('entity_id')
                    title = entity.get('title')
                    name = entity.get('name')
                    last_name = entity.get('lastName')
                    
                    # Преобразуем entity_type_id в enum значение
                    entity_type_enum = get_entity_type_enum(entity_type_id)
                    if not entity_type_enum:
                        logger.warning(f"Неизвестный тип сущности: {entity_type_id}, пропускаю")
                        continue
                    
                    # UPSERT: INSERT ... ON CONFLICT DO UPDATE
                    upsert_query = f"""
                        INSERT INTO {portal_name}_entities (crm_entity_type, entity_id, title, name, lastName)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (crm_entity_type, entity_id) 
                        DO UPDATE SET 
                            title = EXCLUDED.title,
                            name = EXCLUDED.name,
                            lastName = EXCLUDED.lastName
                        RETURNING id;
                    """
                    
                    cursor.execute(upsert_query, (
                        entity_type_enum,
                        entity_id_bitrix,
                        title,
                        name,
                        last_name
                    ))
                    
                    # Получаем внутренний ID
                    internal_id = cursor.fetchone()[0]
                    
                    # Сохраняем маппинг
                    entity_key = (entity_type_id, entity_id_bitrix)
                    entity_id_mapping[entity_key] = internal_id
                    
                    logger.debug(f"Сущность {entity_type_enum}:{entity_id_bitrix} → внутренний ID: {internal_id}")
                
                # Коммитим все изменения
                conn.commit()
                logger.info(f"Успешно обновлено {len(entity_id_mapping)} сущностей в БД")
                
            except Exception as e:
                logger.error(f"Ошибка при обновлении сущностей: {e}")
                conn.rollback()
                return {}
    
    return entity_id_mapping


def get_entity_type_enum(entity_type_id: int) -> str:
    """
    Преобразует entity_type_id в соответствующее ENUM значение.
    
    :param entity_type_id: ID типа сущности (1=LEAD, 2=DEAL, 3=CONTACT, 4=COMPANY)
    :return: Строковое значение для ENUM
    """
    mapping = {
        1: 'LEAD',
        2: 'DEAL', 
        3: 'CONTACT',
        4: 'COMPANY'
    }
    return mapping.get(entity_type_id)


async def update_entities_in_database(
    records_with_entities: dict
) -> dict:
    """
    Обновляет сущности в БД и добавляет внутренние ID к сущностям.
    
    :param records_with_entities: Словарь с записями и сущностями из entity_fetcher
    :return: Тот же словарь с добавленными 'id' полями в сущностях
    """
    logger.info("=== НАЧИНАЮ ОБРАБОТКУ: Обновление сущностей в БД ===")
    logger.info("Шаг 4: Обновляю сущности в базе данных")
    
    result_records = {}
    
    for portal_name, portal_data in records_with_entities.items():
        logger.info(f"Обрабатываю сущности для портала {portal_name}")
        
        entities = portal_data.get('entities', [])
        call_records = portal_data.get('records', [])
        
        # Обновляем сущности в БД и получаем маппинг ID
        entity_id_mapping = upsert_entities_to_db(portal_name, entities)
        
        # Добавляем внутренние ID к сущностям
        enhanced_entities = []
        for entity in entities:
            enhanced_entity = entity.copy()
            
            entity_type_id = entity.get('entity_type_id')
            entity_id_bitrix = entity.get('entity_id')
            
            if entity_type_id and entity_id_bitrix:
                entity_key = (entity_type_id, entity_id_bitrix)
                internal_id = entity_id_mapping.get(entity_key)
                
                if internal_id:
                    enhanced_entity['id'] = internal_id  # Добавляем внутренний ID прямо в сущность
                else:
                    enhanced_entity['id'] = None
            else:
                enhanced_entity['id'] = None
            
            enhanced_entities.append(enhanced_entity)
        
        result_records[portal_name] = {
            'records': call_records,  # Оригинальные записи без изменений
            'entities': enhanced_entities  # Сущности с добавленными внутренними ID
        }
    
    logger.info("=== ЗАВЕРШАЮ ОБРАБОТКУ: Обновление сущностей в БД ===")
    return result_records


def get_entity_type_id_from_string(crm_entity_type: str) -> int:
    """
    Преобразует строковый тип сущности в entity_type_id.
    
    :param crm_entity_type: Строковое значение ('LEAD', 'DEAL', 'CONTACT', 'COMPANY')
    :return: Числовой ID типа сущности
    """
    mapping = {
        'LEAD': 1,
        'DEAL': 2,
        'CONTACT': 3,
        'COMPANY': 4
    }
    return mapping.get(crm_entity_type)


if __name__ == "__main__":
    # Тестовые данные с сущностями
    test_data = {
        "advertpro": {
            "records": [
                {
                    "id": "call_493123",
                    "date": "2025-08-07T10:00:19+03:00",
                    "user_id": "11009",
                    "phone_number": "+79189616367",
                    "entity_id": "27523",
                    "call_type": "1",
                    "crm_entity_type": "CONTACT"
                },
                {
                    "id": "call_493129",
                    "date": "2025-08-07T10:29:33+03:00",
                    "user_id": "11009",
                    "phone_number": "+78612583661",
                    "entity_id": "347831",
                    "call_type": "1",
                    "crm_entity_type": "LEAD"
                }
            ],
            "entities": [
                {
                    "entity_type_id": 3,
                    "entity_id": 27523,
                    "title": "Тестовый контакт",
                    "name": "Иван",
                    "lastName": "Петров"
                },
                {
                    "entity_type_id": 1,
                    "entity_id": 347831,
                    "title": "Тестовый лид",
                    "name": "Мария2222",
                    "lastName": "Сидорова"
                }
            ]
        }
    }
    
    async def test():
        logger.info("Тестирую обновление сущностей в БД")
        
        result = await update_entities_in_database(test_data)
        
        # Сохраняем результат для отладки
        save_debug_json(result, "test_entity_db_update")
        
        logger.info("Тестирование завершено, результат сохранен в bitrix24/json_tests/test_entity_db_update.json")
    
    asyncio.run(test())
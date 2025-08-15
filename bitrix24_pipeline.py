import asyncio
import json
import os
import sys
from datetime import datetime, timedelta

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_fetcher import fetch_data_with_portal_settings
from debug_utils import save_debug_json
from db_client import get_db_client


def fetch_entities_for_records(portal_name: str, records: list) -> list:
    """
    Загружает только те entities, которые связаны с переданными records
    
    :param portal_name: Название портала
    :param records: Список records с полем entity_id
    :return: Список entities только для этих records
    """
    if not records:
        return []
    
    # Собираем уникальные entity_id из records
    entity_ids = set()
    for record in records:
        entity_id = record.get('entity_id')
        if entity_id:
            entity_ids.add(entity_id)
    
    if not entity_ids:
        print(f"Нет entity_id в records для портала {portal_name}")
        return []
    
    print(f"Загружаю {len(entity_ids)} уникальных entities для портала {portal_name}")
    
    # Загружаем entities только для найденных ID
    entities = []
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                # Создаем placeholder для IN запроса
                placeholders = ', '.join(['%s'] * len(entity_ids))
                query = f"""
                    SELECT id, crm_entity_type, entity_id, title, name, lastname, data 
                    FROM {portal_name}_entities 
                    WHERE id = ANY(%s)
                """
                
                cursor.execute(query, (list(entity_ids),))
                rows = cursor.fetchall()
                
                # Преобразуем в словари
                column_names = ['id', 'crm_entity_type', 'entity_id', 'title', 'name', 'lastname', 'data']
                for row in rows:
                    entity = dict(zip(column_names, row))
                    entities.append(entity)
                    
            except Exception as e:
                print(f"Ошибка загрузки entities для портала {portal_name}: {e}")
    
    print(f"Загружено {len(entities)} entities для портала {portal_name}")
    return entities


async def step_1_fetch_records_and_entities_from_db():
    """
    Шаг 1: Подгружает записи из БД за последний days_back согласно bitrix24/bitrix_portals.json
    
    Подгружает:
    - records со статусом 'ready' за последний days_back с полями: id, date, phone_number, data
    - entities с полями: id, crm_entity_type, title, name, lastname, data
    
    :return: Словарь формата {portal_name: {"records": [...], "entities": [...]}}
    """
    print("Шаг 1: Получаю записи и сущности из БД")
    
    # Шаг 1.1: Загружаем records (БЕЗ categories, criteria, entities для оптимизации)
    print("Загружаю records со статусом 'ready'...")
    records_data = fetch_data_with_portal_settings(
        status="ready", 
        fields=["id", "date", "phone_number", "data", "entity_id"],  # ✅ Добавляем entity_id
        analytics_mode=False  # ❌ Отключаем автозагрузку entities
    )
    
    # Шаг 1.2: Загружаем только связанные entities
    result = {}
    for portal_name, portal_data in records_data.items():
        records = portal_data.get("records", [])
        
        print(f"Портал {portal_name}: {len(records)} records")
        
        # Загружаем только entities, связанные с этими records
        entities = fetch_entities_for_records(portal_name, records)
        
        # Убираем entity_id из records (он не нужен в итоговом результате)
        cleaned_records = []
        for record in records:
            cleaned_record = {k: v for k, v in record.items() if k != 'entity_id'}
            cleaned_records.append(cleaned_record)
        
        result[portal_name] = {
            "records": cleaned_records,
            "entities": entities
        }
        
        print(f"Портал {portal_name}: {len(cleaned_records)} записей, {len(entities)} связанных сущностей")
    
    print(f"Загружено данных из {len(result)} порталов")
    return result


async def main():
    """
    Основная функция pipeline для обработки данных Bitrix24
    """
    print("=== Bitrix24 Pipeline ===")
    
    # Шаг 1: Загрузка данных из БД
    data = await step_1_fetch_records_and_entities_from_db()
    
    # Сохраняем результат для отладки
    save_debug_json(data, "bitrix24_pipeline_step1")
    
    print("\n=== Pipeline завершен ===")
    
    # Выводим статистику
    total_records = sum(len(portal_data.get("records", [])) for portal_data in data.values())
    total_entities = sum(len(portal_data.get("entities", [])) for portal_data in data.values())
    
    print(f"Всего обработано:")
    print(f"  - Записей: {total_records}")
    print(f"  - Сущностей: {total_entities}")
    print(f"  - Порталов: {len(data)}")
    
    return data


if __name__ == "__main__":
    asyncio.run(main())

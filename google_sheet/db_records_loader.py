import asyncio
import json
import os
import sys
from datetime import datetime, timedelta

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
                query = f"""
                    SELECT id, crm_entity_type, entity_id, title, name, lastname, data, summary 
                    FROM {portal_name}_entities 
                    WHERE id = ANY(%s)
                """
                
                cursor.execute(query, (list(entity_ids),))
                rows = cursor.fetchall()
                
                # Преобразуем в словари
                column_names = ['id', 'crm_entity_type', 'entity_id', 'title', 'name', 'lastname', 'data', 'summary']
                for row in rows:
                    entity = dict(zip(column_names, row))
                    entities.append(entity)
                    
            except Exception as e:
                print(f"Ошибка загрузки entities для портала {portal_name}: {e}")
    
    print(f"Загружено {len(entities)} entities для портала {portal_name}")
    return entities


def fetch_users_for_records(portal_name: str, records: list) -> list:
    """
    Загружает только тех пользователей, которые связаны с переданными records
    
    :param portal_name: Название портала
    :param records: Список records с полем user_id
    :return: Список users только для этих records
    """
    if not records:
        return []
    
    # Собираем уникальные user_id из records
    user_ids = set()
    for record in records:
        user_id = record.get('user_id')
        if user_id:
            user_ids.add(user_id)
    
    if not user_ids:
        print(f"Нет user_id в records для портала {portal_name}")
        return []
    
    print(f"Загружаю {len(user_ids)} уникальных пользователей для портала {portal_name}")
    
    # Загружаем users только для найденных ID
    users = []
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                query = f"""
                    SELECT id, name, last_name
                    FROM {portal_name}_users 
                    WHERE id = ANY(%s)
                """
                
                cursor.execute(query, (list(user_ids),))
                rows = cursor.fetchall()
                
                # Преобразуем в словари
                column_names = ['id', 'name', 'last_name']
                for row in rows:
                    user = dict(zip(column_names, row))
                    users.append(user)
                    
            except Exception as e:
                print(f"Ошибка загрузки пользователей для портала {portal_name}: {e}")
    
    print(f"Загружено {len(users)} пользователей для портала {portal_name}")
    return users


def fetch_categories_for_portal(portal_name: str) -> list:
    """
    Загружает все категории для портала
    
    :param portal_name: Название портала
    :return: Список категорий
    """
    categories = []
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                query = f"""
                    SELECT id, name
                    FROM {portal_name}_categories
                    ORDER BY id
                """
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Преобразуем в словари
                column_names = ['id', 'name']
                for row in rows:
                    category = dict(zip(column_names, row))
                    categories.append(category)
                    
            except Exception as e:
                print(f"Ошибка загрузки категорий для портала {portal_name}: {e}")
    
    print(f"Загружено {len(categories)} категорий для портала {portal_name}")
    return categories


def fetch_criteria_for_portal(portal_name: str) -> list:
    """
    Загружает все критерии для портала
    
    :param portal_name: Название портала
    :return: Список критериев
    """
    criteria = []
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                query = f"""
                    SELECT id, name, group_id, show_text_description, evaluate_criterion, include_in_score, include_in_entity_description
                    FROM {portal_name}_criteria
                    ORDER BY id
                """
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Преобразуем в словари
                column_names = ['id', 'name', 'group_id', 'show_text_description', 'evaluate_criterion', 'include_in_score', 'include_in_entity_description']
                for row in rows:
                    criterion = dict(zip(column_names, row))
                    criteria.append(criterion)
                    
            except Exception as e:
                print(f"Ошибка загрузки критериев для портала {portal_name}: {e}")
    
    print(f"Загружено {len(criteria)} критериев для портала {portal_name}")
    return criteria


def fetch_criterion_groups_for_portal(portal_name: str) -> list:
    """
    Загружает все группы критериев для портала
    
    :param portal_name: Название портала
    :return: Список групп критериев
    """
    criterion_groups = []
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                query = f"""
                    SELECT id, name
                    FROM {portal_name}_criterion_groups
                    ORDER BY id
                """
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Преобразуем в словари
                column_names = ['id', 'name']
                for row in rows:
                    group = dict(zip(column_names, row))
                    criterion_groups.append(group)
                    
            except Exception as e:
                print(f"Ошибка загрузки групп критериев для портала {portal_name}: {e}")
    
    print(f"Загружено {len(criterion_groups)} групп критериев для портала {portal_name}")
    return criterion_groups


async def load_records_entities_and_users():
    """
    Загружает записи, сущности, пользователей, критерии, категории и группы критериев из БД
    
    Подгружает:
    - records со статусом 'ready' за последний days_back с полями: id, date, phone_number, dialogue, summary, data, user_id, entity_id
    - entities с полями: id, crm_entity_type, title, name, lastname, data, summary
    - users с полями: id, name, last_name
    - categories с полями: id, name
    - criteria с полями: id, name, group_id, show_text_description, evaluate_criterion, include_in_score, include_in_entity_description
    - criterion_groups с полями: id, name
    
    :return: Словарь формата {portal_name: {"records": [...], "entities": [...], "users": [...], "categories": [...], "criteria": [...], "criterion_groups": [...]}}
    """
    print("Загружаю записи, сущности, пользователей, критерии, категории и группы критериев из БД")
    
    # Шаг 1.1: Загружаем records (БЕЗ categories, criteria для оптимизации)
    print("Загружаю records со статусом 'ready'...")
    records_data = fetch_data_with_portal_settings(
        status="ready", 
        fields=["id", "date", "phone_number", "dialogue", "summary", "data", "entity_id", "user_id"],  # ✅ Добавляем dialogue и summary
        analytics_mode=False  # ❌ Отключаем автозагрузку
    )
    
    # Шаг 1.2: Загружаем только связанные entities и users
    result = {}
    for portal_name, portal_data in records_data.items():
        records = portal_data.get("records", [])
        
        print(f"Портал {portal_name}: {len(records)} records")
        
        # Загружаем только entities, связанные с этими records
        entities = fetch_entities_for_records(portal_name, records)
        
        # Загружаем только пользователей, связанных с этими records
        users = fetch_users_for_records(portal_name, records)
        
        # Загружаем все категории для портала
        categories = fetch_categories_for_portal(portal_name)
        
        # Загружаем все критерии для портала
        criteria = fetch_criteria_for_portal(portal_name)
        
        # Загружаем все группы критериев для портала
        criterion_groups = fetch_criterion_groups_for_portal(portal_name)
        
        # Оставляем все поля в records, включая user_id и entity_id для связывания
        cleaned_records = records
        
        result[portal_name] = {
            "records": cleaned_records,
            "entities": entities,
            "users": users,
            "categories": categories,
            "criteria": criteria,
            "criterion_groups": criterion_groups
        }
        
        print(f"Портал {portal_name}: {len(cleaned_records)} записей, {len(entities)} сущностей, {len(users)} пользователей, {len(categories)} категорий, {len(criteria)} критериев, {len(criterion_groups)} групп критериев")
    
    print(f"Загружено данных из {len(result)} порталов")
    return result


async def main():
    """
    Основная функция для загрузки записей из БД
    """
    print("=== DB Records Loader ===")
    
    # Загрузка данных из БД
    data = await load_records_entities_and_users()
    
    # Сохраняем результат для отладки
    save_debug_json(data, "db_records_loader")
    
    print("\n=== Загрузка записей завершена ===")
    
    # Выводим статистику
    total_records = sum(len(portal_data.get("records", [])) for portal_data in data.values())
    total_entities = sum(len(portal_data.get("entities", [])) for portal_data in data.values())
    total_users = sum(len(portal_data.get("users", [])) for portal_data in data.values())
    total_categories = sum(len(portal_data.get("categories", [])) for portal_data in data.values())
    total_criteria = sum(len(portal_data.get("criteria", [])) for portal_data in data.values())
    total_criterion_groups = sum(len(portal_data.get("criterion_groups", [])) for portal_data in data.values())
    
    print(f"Всего обработано:")
    print(f"  - Записей: {total_records}")
    print(f"  - Сущностей: {total_entities}")
    print(f"  - Пользователей: {total_users}")
    print(f"  - Категорий: {total_categories}")
    print(f"  - Критериев: {total_criteria}")
    print(f"  - Групп критериев: {total_criterion_groups}")
    print(f"  - Порталов: {len(data)}")
    
    return data


if __name__ == "__main__":
    asyncio.run(main())

import json
from db_client import get_db_client
from upload.insert_db_item import check_and_insert_entity


def upload_records_from_dict(data_dict: dict, status: str):
    """
    Функция принимает на вход статус (status) и словарь вида:
    {
        "advertpro": [{...}, {...}, ... ],
        "kilimandjaro": [{...}, ... ],
        ...
    }
    где каждый ключ (advertpro, kilimandjaro и т.д.) — это имя таблицы в БД.

    Логика обновления:
    1. Если 'data' нет или оно пустое, пропускаем обновление data и статуса.
    2. Если 'data' есть, проверяем массив 'criteria' внутри data на полноту:
       - должны присутствовать поля: name, description, isEvaluated, evaluation, text.
       - если хотя бы одно отсутствует — обновляем только data (статус не трогаем).
       - если все поля есть во всех объектах 'criteria' — обновляем data и ставим status равным аргументу функции (status).
    По окончании всех обновлений вызываем commit.
    """

    required_fields = ["name", "text", "evaluation"]

    with get_db_client() as conn:
        with conn.cursor() as cur:
            # Перебираем все таблицы (ключи верхнего уровня)
            for table_name, records in data_dict.items():
                # Перебираем каждую запись в списке
                for record in records:
                    # Проверяем, есть ли поле 'data'
                    record_data = record.get("data")
                    if not record_data:
                        # Если data нет или оно пустое — ничего не обновляем, идём дальше
                        continue

                    # Предполагаем, что нужно проверить массив criteria
                    criteria_list = record_data.get("criteria")

                    # Если criteria нет или оно не список — тогда считаем, что данных недостаточно
                    # Обновляем только data (без изменения статуса).
                    if not isinstance(criteria_list, list):
                        update_query = f"""
                            UPDATE {table_name}
                            SET data = %s
                            WHERE id = %s
                        """
                        cur.execute(update_query, [json.dumps(record_data), record["id"]])
                        continue

                    # Проверяем наличие всех обязательных полей в каждом элементе criteria
                    all_criteria_ok = True
                    for crit in criteria_list:
                        if not all(field in crit for field in required_fields):
                            all_criteria_ok = False
                            break

                    if all_criteria_ok:
                        # Все поля в criteria на месте => обновляем data и ставим статус = 'status' из аргумента
                        update_query = f"""
                            UPDATE {table_name}
                            SET data = %s,
                                status = %s
                            WHERE id = %s
                        """
                        cur.execute(update_query, [json.dumps(record_data), status, record["id"]])
                    else:
                        # Какие-то поля не хватает => обновляем только data
                        update_query = f"""
                            UPDATE {table_name}
                            SET data = %s
                            WHERE id = %s
                        """
                        cur.execute(update_query, [json.dumps(record_data), record["id"]])

            # Фиксируем изменения в БД
            conn.commit()


def upload_full_data_from_dict(data_dict: dict, status: str):
    """
    Функция принимает на вход статус (status) и полный словарь данных вида:
    {
        "advertpro": {
            "records": [{...}, {...}, ...],
            "entities": [{...}, {...}, ...],
            "criteria": [...],
            "categories": [...]
        },
        "kilimandjaro": {
            "records": [{...}, ...],
            "entities": [{...}, ...],
            ...
        },
        ...
    }
    
    Загружает:
    1. records в основные таблицы через upload_records_from_dict
    2. entities в таблицы {table_name}_entities через check_and_insert_entity
    """
    print("Загружаю records в БД...")
    # Извлекаем только records для загрузки в основные таблицы
    records_only = {table: data["records"] for table, data in data_dict.items() if "records" in data}
    upload_records_from_dict(records_only, status)
    
    print("Загружаю entities в БД...")
    # Загружаем entities в таблицы entities
    for table_name, table_data in data_dict.items():
        if "entities" in table_data and table_data["entities"]:
            for entity in table_data["entities"]:
                entity_id = entity.get("id")
                entity_data = entity.get("data", {})
                if entity_id:
                    check_and_insert_entity(table_name, entity_id, entity_data)
            print(f"Загружено {len(table_data['entities'])} entities для таблицы {table_name}")
    
    print("Загрузка records и entities завершена!")


# Пример использования
if __name__ == "__main__":
    # Загружаем реальные данные из json_tests/final_records.json
    import json
    
    with open('json_tests/final_records.json', 'r', encoding='utf-8') as f:
        dialogue_dict = json.load(f)
    
    print("Загружены данные из final_records.json:")
    for table_name, table_data in dialogue_dict.items():
        records_count = len(table_data.get("records", []))
        entities_count = len(table_data.get("entities", []))
        print(f"  {table_name}: {records_count} records, {entities_count} entities")
    
    # Тест новой функции с полной структурой
    print("\n" + "="*50)
    print("ТЕСТ: upload_full_data_from_dict с реальными данными")
    print("="*50)
    upload_full_data_from_dict(dialogue_dict, status='ready')
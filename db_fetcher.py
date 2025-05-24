import json
from db_client import get_db_client


def get_tables_with_status_column():
    """
    Возвращает список таблиц в схеме 'public', у которых есть колонка 'status'.
    """
    conn = get_db_client()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND column_name = 'status';
    """)
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables


def fetch_data(status: str, fields: list[str], analytics_mode: bool) -> dict:
    """
    Получает из всех таблиц (где присутствует колонка 'status')
    записи с указанным значением 'status', выбирая только указанные в `fields` колонки.
    Если analytics_mode=True, загружаются также связанные данные из категорий, критериев и сущностей.

    Итоговая структура:
        {
            "advertpro": {
                "records": [...],            # записи основной таблицы
                "categories": [              # данные из таблицы advertpro_categories,
                    {                      # у каждой записи добавлено поле criteria – список id критериев,
                        "id": ...,
                        ...,
                        "criteria": [id1, id2, ...]
                    },
                    ...
                ],
                "criteria": [...],           # данные из таблицы advertpro_criteria
                "entities": [...]            # данные из таблицы advertpro_entities
            },
            ...
        }

    :param status: Значение в колонке 'status', по которому выбираем строки (например, 'uploaded').
    :param fields: Список колонок, которые нужно вернуть (например, ['id', 'dialogue']).
    :param analytics_mode: Флаг загрузки дополнительных данных.
    :return: Итоговая структура с данными.
    """
    tables = get_tables_with_status_column()
    conn = get_db_client()
    cursor = conn.cursor()

    result = {}
    table_entity_ids = {table: set() for table in tables}

    for table in tables:
        # Формируем список колонок для выборки
        columns_sql = ", ".join(fields)
        # Если analytics_mode=True и 'entity_id' не указан явно, добавляем его для последующей выборки
        if analytics_mode and "entity_id" not in fields:
            columns_sql += ", entity_id"

        query = f"SELECT {columns_sql} FROM {table} WHERE status = %s;"
        cursor.execute(query, (status,))
        rows = cursor.fetchall()

        records = []
        for row in rows:
            record = {}
            # Заполняем словарь для указанных полей
            for i, field in enumerate(fields):
                record[field] = row[i]
            # Если analytics_mode=True и 'entity_id' не был передан – добавляем его из последнего столбца
            if analytics_mode and "entity_id" not in fields:
                record["entity_id"] = row[-1]
            if "entity_id" in record:
                table_entity_ids[table].add(record["entity_id"])
            records.append(record)

        # Формируем результирующую структуру для таблицы:
        # Если analytics_mode=False – только "records"
        if analytics_mode:
            result[table] = {
                "records": records,
                "categories": [],
                "criteria": [],
                "entities": []
            }
        else:
            result[table] = {"records": records}

        # Если включен analytics_mode, загружаем связанные данные
        if analytics_mode:
            # Загружаем категории
            try:
                cursor.execute(f"SELECT * FROM {table}_categories;")
                categories = [dict(zip([desc[0] for desc in cursor.description], row))
                              for row in cursor.fetchall()]
            except Exception:
                categories = []

            # Загружаем критерии
            try:
                cursor.execute(f"SELECT * FROM {table}_criteria;")
                criteria = [dict(zip([desc[0] for desc in cursor.description], row))
                            for row in cursor.fetchall()]
            except Exception:
                criteria = []

            # Загружаем связывающую таблицу для категорий и критериев
            try:
                cursor.execute(f"SELECT * FROM {table}_categories_criteria;")
                categories_criteria = [dict(zip([desc[0] for desc in cursor.description], row))
                                       for row in cursor.fetchall()]
            except Exception:
                categories_criteria = []

            # Создаём маппинг: category_id -> список criterion_id
            cat_to_criteria = {}
            for link in categories_criteria:
                cat_id = link.get("category_id") or link.get("id_category")
                crit_id = link.get("criterion_id") or link.get("id_criteria")
                if cat_id is not None and crit_id is not None:
                    cat_to_criteria.setdefault(cat_id, []).append(crit_id)

            # Для каждой категории добавляем новое поле 'criteria'
            for cat in categories:
                cat_id = cat.get("id")
                cat["criteria"] = cat_to_criteria.get(cat_id, [])

            result[table]["categories"] = categories
            result[table]["criteria"] = criteria

    # Если analytics_mode=True, подгружаем данные сущностей для каждой таблицы
    if analytics_mode:
        for table in tables:
            entity_ids = list(table_entity_ids[table])
            entities = []
            if entity_ids:
                try:
                    query = f"SELECT * FROM {table}_entities WHERE id = ANY(%s);"
                    cursor.execute(query, (entity_ids,))
                    entities = [dict(zip([desc[0] for desc in cursor.description], row))
                                for row in cursor.fetchall()]
                except Exception:
                    entities = []
            result[table]["entities"] = entities

    cursor.close()
    conn.close()
    return result


if __name__ == "__main__":
    records = fetch_data(
        status="uploaded",
        fields=["id", "dialogue", "entity_id", "data"],
        analytics_mode=True)
    print(json.dumps(records, indent=4, ensure_ascii=False))
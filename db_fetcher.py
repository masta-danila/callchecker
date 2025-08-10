import json
import os
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


def fetch_data_with_portal_settings(status: str = None, fields: list[str] = None, analytics_mode: bool = False) -> dict:
    """
    Получает данные из БД с учетом индивидуальных настроек days_back для каждого портала.
    
    :param status: Статус записей для фильтрации
    :param fields: Поля для выборки 
    :param analytics_mode: Режим аналитики
    :return: Словарь с данными по порталам
    """
    # Читаем конфигурацию порталов
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bitrix24", "bitrix_portals.json")
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Ошибка чтения конфигурации порталов: {e}")
        return {}

    default_days_back = config.get('default_settings', {}).get('days_back', 3)
    portals = config.get('portals', [])
    
    # Создаем мапинг: portal_name -> days_back
    portal_days_map = {}
    for portal_config in portals:
        if isinstance(portal_config, str):
            portal_url = portal_config
            portal_days_back = default_days_back
        else:
            portal_url = portal_config.get('url', '')
            portal_days_back = portal_config.get('days_back', default_days_back)
        
        try:
            portal_name = portal_url.split('/')[2].split('.')[0]
            portal_days_map[portal_name] = portal_days_back
        except:
            continue

    # Получаем данные для каждого портала с его индивидуальными настройками
    result = {}
    conn = get_db_client()
    cursor = conn.cursor()
    
    tables = get_tables_with_status_column()
    
    for table in tables:
        # Проверяем, есть ли для этой таблицы настройки портала
        portal_days_back = portal_days_map.get(table, default_days_back)
        
        # Формируем список колонок для выборки
        if fields is None:
            columns_sql = "*"
        else:
            columns_sql = ", ".join(fields)
            if analytics_mode and "entity_id" not in fields:
                columns_sql += ", entity_id"

        # Формируем WHERE условие с учетом days_back для портала
        where_conditions = []
        params = []
        
        if status is not None:
            where_conditions.append("status = %s")
            params.append(status)
        
        # Добавляем фильтр по дате с индивидуальным days_back
        where_conditions.append(f"date >= CURRENT_DATE - INTERVAL '{portal_days_back} days'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        query = f"SELECT {columns_sql} FROM {table} {where_clause};"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Получаем имена колонок
        column_names = [desc[0] for desc in cursor.description]

        # Преобразуем строки в словари
        records = []
        for row in rows:
            record = dict(zip(column_names, row))
            records.append(record)

        result[table] = {"records": records}

        # Если включен режим аналитики, загружаем связанные данные
        if analytics_mode:
            # Загружаем категории
            try:
                cursor.execute(f"SELECT * FROM {table}_categories")
                categories = [dict(zip([desc[0] for desc in cursor.description], row)) 
                            for row in cursor.fetchall()]
                result[table]["categories"] = categories
            except:
                result[table]["categories"] = []

            # Загружаем критерии  
            try:
                cursor.execute(f"SELECT * FROM {table}_criteria")
                criteria = [dict(zip([desc[0] for desc in cursor.description], row))
                          for row in cursor.fetchall()]
                result[table]["criteria"] = criteria
            except:
                result[table]["criteria"] = []

            # Загружаем сущности
            try:
                cursor.execute(f"SELECT * FROM {table}_entities")
                entities = [dict(zip([desc[0] for desc in cursor.description], row))
                          for row in cursor.fetchall()]
                result[table]["entities"] = entities
            except:
                result[table]["entities"] = []

        print(f"Таблица {table}: загружено {len(records)} записей за последние {portal_days_back} дней")

    cursor.close()
    conn.close()
    return result


def fetch_data(status: str = None, fields: list[str] = None, analytics_mode: bool = False) -> dict:
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
                   Если None, то выбираются все записи независимо от статуса.
    :param fields: Список колонок, которые нужно вернуть (например, ['id', 'dialogue']).
                   Если None, то возвращаются все колонки.
    :param analytics_mode: Флаг загрузки дополнительных данных.
    :param days_back: Количество дней назад для фильтрации по дате (опционально).
    :return: Итоговая структура с данными.
    """
    tables = get_tables_with_status_column()
    conn = get_db_client()
    cursor = conn.cursor()

    result = {}
    table_entity_ids = {table: set() for table in tables}

    for table in tables:
        # Формируем список колонок для выборки
        if fields is None:
            columns_sql = "*"
        else:
            columns_sql = ", ".join(fields)
            # Если analytics_mode=True и 'entity_id' не указан явно, добавляем его для последующей выборки
            if analytics_mode and "entity_id" not in fields:
                columns_sql += ", entity_id"

        # Формируем WHERE условие
        where_conditions = []
        params = []
        
        if status is not None:
            where_conditions.append("status = %s")
            params.append(status)
        

        
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
            query = f"SELECT {columns_sql} FROM {table} {where_clause};"
        else:
            query = f"SELECT {columns_sql} FROM {table};"
        
        cursor.execute(query, params)
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
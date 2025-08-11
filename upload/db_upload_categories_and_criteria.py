import os
import json
import sys

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_db_client


def insert_data_into_multiple_tables(
    table_names: list,
    criterion_groups_file: str = "criterion_groups.json",
    criteria_file: str = "criteria.json",
    categories_file: str = "categories.json"
):
    """
    Считывает данные из JSON-файлов (criterion_groups.json, criteria.json, categories.json)
    и вставляет их в таблицы (groups, criteria, categories и связующую таблицу)
    для каждого названия из списка table_names.

    То есть, если на вход дан список ["advertpro", "some_table"], то заполнение
    пройдет в таблицы:
      - advertpro_criterion_groups, advertpro_criteria, advertpro_categories, advertpro_categories_criteria
      - some_table_criterion_groups, some_table_criteria, some_table_categories, some_table_categories_criteria

    Параметры:
        table_names (list[str]): список строк (имен основных таблиц).
        criterion_groups_file (str): Путь к JSON с группами критериев.
        criteria_file (str): Путь к JSON с критериями.
        categories_file (str): Путь к JSON с категориями.
    """
    # Получаем путь к директории текущего скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Формируем пути к JSON файлам
    criterion_groups_file = os.path.join(script_dir, criterion_groups_file)
    criteria_file = os.path.join(script_dir, criteria_file)
    categories_file = os.path.join(script_dir, categories_file)

    # Считаем данные из файлов ОДИН раз, а не в цикле, чтобы не перечитывать каждый раз
    with open(criterion_groups_file, "r", encoding="utf-8") as f:
        criterion_groups_data = json.load(f)

    with open(criteria_file, "r", encoding="utf-8") as f:
        criteria_data = json.load(f)

    with open(categories_file, "r", encoding="utf-8") as f:
        categories_data = json.load(f)

    # Выполняем вставку в одной транзакции
    try:
        with get_db_client() as conn:
            with conn.cursor() as cur:
                for table_name in table_names:
                    print(f"--- Начало вставки данных в набор таблиц для: {table_name} ---")

                    # 1) Заполняем таблицу групп критериев: {table_name}_criterion_groups
                    for group in criterion_groups_data["criterion_groups"]:
                        insert_query = f"""
                            INSERT INTO {table_name}_criterion_groups (name)
                            VALUES (%s);
                        """
                        cur.execute(insert_query, (group["name"],))

                    print(f"Данные в таблицу {table_name}_criterion_groups добавлены.")

                    # 2) Заполняем таблицу критериев: {table_name}_criteria
                    for criterion in criteria_data["criteria"]:
                        insert_query = f"""
                            INSERT INTO {table_name}_criteria
                                (group_id, name, prompt, show_text_description,
                                 evaluate_criterion, include_in_score,
                                 include_in_entity_description, llm_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        cur.execute(
                            insert_query,
                            (
                                criterion["group_id"],
                                criterion["name"],  # Добавляем поле name
                                criterion["prompt"],
                                criterion["show_text_description"],
                                criterion["evaluate_criterion"],
                                criterion["include_in_score"],
                                criterion["include_in_entity_description"],
                                criterion["llm_type"]
                            )
                        )

                    print(f"Данные в таблицу {table_name}_criteria добавлены.")

                    # 3) Заполняем таблицу категорий и связующую таблицу categories_criteria
                    for category in categories_data["categories"]:
                        insert_category_query = f"""
                            INSERT INTO {table_name}_categories (name, prompt)
                            VALUES (%s, %s)
                            RETURNING id;
                        """
                        # Обратите внимание, что теперь передаем и name, и prompt
                        cur.execute(insert_category_query, (category["name"], category["prompt"]))
                        new_category_id = cur.fetchone()[0]

                        # Для каждой категории вставим связи с критериями
                        for crit_id in category["criteria"]:
                            insert_link_query = f"""
                                INSERT INTO {table_name}_categories_criteria (category_id, criterion_id)
                                VALUES (%s, %s);
                            """
                            cur.execute(insert_link_query, (new_category_id, crit_id))

                    print(f"Данные в таблицы {table_name}_categories и {table_name}_categories_criteria добавлены.")

                    print(f"--- Вставка данных в набор таблиц для {table_name} завершена ---\n")

            # Если всё прошло без ошибок, фиксируем изменения
            conn.commit()

    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        raise


if __name__ == "__main__":
    # Пример вызова
    try:
        insert_data_into_multiple_tables(
            ["advertpro"],
            criterion_groups_file="criterion_groups.json",
            criteria_file="criteria.json",
            categories_file="categories.json"
        )
    except Exception as error:
        print(f"Ошибка: {error}")
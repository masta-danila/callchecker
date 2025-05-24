import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_client import get_db_client


def create_tables(*table_names):
    """
    Функция для создания нескольких таблиц с указанными именами.
    Если таблица существует, она удаляется и создаётся заново.

    :param table_names: Названия таблиц
    """
    # Создание ENUM типа для статуса, если он ещё не существует
    create_status_enum_query = """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status_enum') THEN
                CREATE TYPE status_enum AS ENUM ('uploaded', 'recognized', 'fixed', 'ready');
            END IF;
        END $$;
    """

    # Создание ENUM типа для llm_type, если он ещё не существует
    create_llm_type_enum_query = """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'llm_type_enum') THEN
                CREATE TYPE llm_type_enum AS ENUM ('standard', 'premium');
            END IF;
        END $$;
    """

    try:
        with get_db_client() as conn:
            with conn.cursor() as cur:
                # Проверка или создание ENUM типов
                cur.execute(create_status_enum_query)
                print("Тип status_enum проверен или создан.")
                cur.execute(create_llm_type_enum_query)
                print("Тип llm_type_enum проверен или создан.")

                for table_name in table_names:
                    # -------------------------------------------
                    # Удаляем таблицы (порядок удаления не важен благодаря CASCADE)
                    # -------------------------------------------
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}_categories_criteria CASCADE;")
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}_categories CASCADE;")
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}_criteria CASCADE;")
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}_criterion_groups CASCADE;")
                    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}_entities CASCADE;")

                    # -------------------------------------------
                    # Создаём таблицу сущностей (entities) первой, чтобы можно было установить внешний ключ в основной таблице
                    # -------------------------------------------
                    create_entities_table = f"""
                        CREATE TABLE {table_name}_entities (
                            id INTEGER PRIMARY KEY,  -- id не автоинкремент; должен совпадать с entity_id из основной таблицы
                            data JSONB               -- Дополнительные данные о сущности
                        );
                    """
                    cur.execute(create_entities_table)
                    print(f"Таблица {table_name}_entities успешно создана.")

                    # -------------------------------------------
                    # Создаём основную таблицу с внешним ключом на таблицу сущностей
                    # -------------------------------------------
                    create_main_table_query = f"""
                        CREATE TABLE {table_name} (
                            id TEXT PRIMARY KEY NOT NULL,       -- Обязательное поле (ID звонка)
                            date TIMESTAMP NOT NULL,            -- Обязательное поле (Дата звонка)
                            dialogue TEXT,                      -- Необязательное поле (Текст диалога)
                            data JSONB,                         -- Необязательное поле (Дополнительные данные)
                            status status_enum NOT NULL,        -- Обязательное поле (ENUM для статусов)
                            audio_metadata JSONB,               -- Необязательное поле (Метаинформация об аудио)
                            user_id INTEGER,                    -- Целое число (ID пользователя)
                            entity_id INTEGER NOT NULL,         -- Обязательное поле (ID сущности)
                            FOREIGN KEY (entity_id)
                                REFERENCES {table_name}_entities(id)
                        );
                    """
                    cur.execute(create_main_table_query)
                    print(f"Таблица {table_name} успешно создана.")

                    # -------------------------------------------
                    # Создаём таблицу групп критериев
                    # -------------------------------------------
                    create_criterion_groups_table = f"""
                        CREATE TABLE {table_name}_criterion_groups (
                            id SERIAL PRIMARY KEY,               -- Уникальный идентификатор группы (автоинкремент)
                            name VARCHAR(255) NOT NULL           -- Название группы критериев
                        );
                    """
                    cur.execute(create_criterion_groups_table)
                    print(f"Таблица {table_name}_criterion_groups успешно создана.")

                    # -------------------------------------------
                    # Создаём таблицу критериев (добавлено поле name)
                    # -------------------------------------------
                    create_criteria_table = f"""
                        CREATE TABLE {table_name}_criteria (
                            id SERIAL PRIMARY KEY,                               -- Уникальный идентификатор критерия (автоинкремент)
                            group_id INTEGER NOT NULL,                           -- Ссылка на группу критериев
                            name VARCHAR(255) NOT NULL,                          -- Название критерия
                            prompt TEXT NOT NULL,                                -- Текст промпта для нейросети
                            show_text_description BOOLEAN DEFAULT TRUE,          -- Отображать ли текстовое описание
                            evaluate_criterion BOOLEAN DEFAULT TRUE,             -- Оценивать ли данный критерий
                            include_in_score BOOLEAN DEFAULT TRUE,               -- Учитывать ли оценку критерия в общей оценке
                            include_in_entity_description BOOLEAN DEFAULT FALSE, -- Использовать ли критерий в описании сущности
                            llm_type llm_type_enum NOT NULL,                     -- Тип нейросети: "standard" или "premium"
                            FOREIGN KEY (group_id)
                                REFERENCES {table_name}_criterion_groups(id)
                                ON DELETE CASCADE
                        );
                    """
                    cur.execute(create_criteria_table)
                    print(f"Таблица {table_name}_criteria успешно создана.")

                    # -------------------------------------------
                    # Создаём таблицу категорий (добавлено поле name)
                    # -------------------------------------------
                    create_categories_table = f"""
                        CREATE TABLE {table_name}_categories (
                            id SERIAL PRIMARY KEY,   -- Уникальный идентификатор категории (автоинкремент)
                            name VARCHAR(255) NOT NULL, -- Название категории
                            prompt TEXT NOT NULL     -- Текст промпта для нейросети
                        );
                    """
                    cur.execute(create_categories_table)
                    print(f"Таблица {table_name}_categories успешно создана.")

                    # -------------------------------------------
                    # Создаём связующую таблицу "категории - критерий"
                    # -------------------------------------------
                    create_categories_criteria_table = f"""
                        CREATE TABLE {table_name}_categories_criteria (
                            category_id INTEGER NOT NULL,
                            criterion_id INTEGER NOT NULL,
                            PRIMARY KEY (category_id, criterion_id),
                            FOREIGN KEY (category_id)
                                REFERENCES {table_name}_categories(id)
                                ON DELETE CASCADE,
                            FOREIGN KEY (criterion_id)
                                REFERENCES {table_name}_criteria(id)
                                ON DELETE CASCADE
                        );
                    """
                    cur.execute(create_categories_criteria_table)
                    print(f"Таблица {table_name}_categories_criteria успешно создана.")

    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise


# Пример вызова функции
if __name__ == "__main__":
    try:
        create_tables("advertpro", "test")
    except Exception as error:
        print(f"Ошибка: {error}")
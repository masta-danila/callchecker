import os
import sys
import json

# Добавляем родительскую директорию в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Добавляем текущую директорию
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from get_files_dict import get_files_dict
from upload import upload_file_to_storage
from audio_metadata import get_audio_metadata
from insert_db_item import insert_data, check_and_insert_entity
from datetime_extract import extract_datetime_from_filename
from db_upload_categories_and_criteria import insert_data_into_multiple_tables


def process_audio_files(audio_folder: str, db_tables: list):
    """
    Обрабатывает файлы из папки audio и загружает их в облако,
    получает метаданные и записывает в базу данных.

    Args:
        audio_folder (str): Путь к папке с аудио файлами.
        db_tables (list): Список таблиц для записи данных.
    """
    # Получаем абсолютный путь к папке audio относительно текущего скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    audio_folder_path = os.path.join(script_dir, audio_folder)

    # 1. Получаем словарь файлов из указанной папки
    try:
        files_dict = get_files_dict(audio_folder_path)
        print(f"Список файлов в папке {audio_folder_path}: {files_dict}")
    except Exception as e:
        print(f"Ошибка при получении файлов из папки {audio_folder_path}: {e}")
        return

    # Словарь для хранения "ключей" (строк после '+') и соответствующих entity_id
    entity_map = {}
    # Следующее доступное значение entity_id (начинаем с 1)
    next_entity_id = 1

    # 2. Цикл по всем найденным аудиофайлам
    for file_name, file_path in files_dict.items():
        try:
            # Извлечение даты и времени из названия файла
            call_datetime = extract_datetime_from_filename(file_name)
            print(f"Дата и время звонка извлечены: {call_datetime}")

            # Загрузка файла в облако
            print(f"Загружаем файл: {file_name}")
            uri = upload_file_to_storage(file_path)

            # Получение метаданных аудио
            print(f"Получаем метаданные для файла: {file_name}")
            metadata = get_audio_metadata(file_path)
            print(metadata)

            # Фиксированный user_id
            user_id = 1

            # Определяем entity_id по логике «строк после +»
            plus_index = file_name.find('+')
            if plus_index != -1:
                entity_key = file_name[plus_index + 1:]
                if entity_key in entity_map:
                    entity_id = entity_map[entity_key]
                else:
                    entity_id = next_entity_id
                    entity_map[entity_key] = entity_id
                    next_entity_id += 1
            else:
                entity_id = next_entity_id
                next_entity_id += 1

            # Перед записью данных проверяем наличие сущности в таблице сущностей для каждой таблицы
            for table in db_tables:
                check_and_insert_entity(table, entity_id)
                print(f"Записываем данные в таблицу: {table}")
                insert_data(
                    table_name=table,
                    file_name=file_name,
                    metadata=metadata,
                    uri=uri,
                    date=call_datetime,
                    user_id=user_id,
                    entity_id=entity_id
                )

            print(f"Файл {file_name} успешно обработан.\n")

        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {e}")


if __name__ == "__main__":
    # Папка с аудио файлами
    audio_folder = "audio"

    # Список таблиц для записи
    db_tables = ["advertpro", "test"]

    # Запуск обработки файлов
    process_audio_files(audio_folder, db_tables)

    # Добавление категорий и критериев
    insert_data_into_multiple_tables(table_names=db_tables)
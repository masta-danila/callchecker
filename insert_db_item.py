import json
from datetime import datetime
from db_client import get_db_client
from logger_config import setup_logger

# Настройка логгера для этого модуля
logger = setup_logger('insert_db_item', 'logs/insert_db_item.log')


def insert_data(table_name: str,
                file_name: str,
                metadata: dict,
                uri: str,
                date: datetime,
                user_id: int,
                entity_id: int):
    """
    Добавляет запись в таблицу базы данных с аудио-метаданными.

    Args:
        table_name (str): Название таблицы.
        file_name (str): Имя файла (будет использоваться как id).
        metadata (dict): Метаданные аудио.
        uri (str): URI аудио файла.
        date (datetime): Дата и время звонка.
        user_id (int): Идентификатор пользователя (в задаче = 1).
        entity_id (int): Идентификатор сущности (сквозной счётчик или ключ).
    """
    # Добавляем URI в метаданные
    metadata['uri'] = uri

    # Статус по умолчанию
    status = 'uploaded'

    # Создаем подключение к базе данных
    conn = get_db_client()
    cursor = conn.cursor()

    try:
        # Вставляем только нужные поля — без criteria
        query = f"""
        INSERT INTO {table_name} (
            id, 
            date, 
            dialogue, 
            data, 
            status, 
            audio_metadata,
            user_id,
            entity_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Параметры для вставки
        params = (
            file_name,  # id
            date,  # date
            None,  # dialogue
            None,  # data
            status,  # status (ENUM)
            json.dumps(metadata),  # audio_metadata
            user_id,  # user_id
            entity_id  # entity_id
        )

        # Выполняем запрос на вставку
        cursor.execute(query, params)
        conn.commit()

        logger.info(f"Запись успешно добавлена в таблицу {table_name}: "
                    f"ID={file_name}, URI={uri}, user_id={user_id}, entity_id={entity_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при добавлении записи: {e}")
    finally:
        cursor.close()
        conn.close()


def check_and_insert_entity(table_name: str, entity_id: int, entity_data: dict = None, summary: str = None):
    """
    Проверяет, существует ли запись с указанным entity_id в таблице сущностей.
    Если записи нет, добавляет новую запись.

    Args:
        table_name (str): Название основной таблицы (без постфикса _entities).
        entity_id (int): Идентификатор сущности для проверки.
        entity_data (dict, optional): Дополнительные данные для поля data.
        summary (str, optional): Краткое резюме сущности для поля summary.
    """
    conn = get_db_client()
    cursor = conn.cursor()
    try:
        # Проверка наличия записи в таблице сущностей
        query = f"SELECT id FROM {table_name}_entities WHERE id = %s"
        cursor.execute(query, (entity_id,))
        result = cursor.fetchone()

        if not result:
            # Если запись не найдена, добавляем новую запись
            insert_query = f"INSERT INTO {table_name}_entities (id, data, summary) VALUES (%s, %s, %s)"
            data_json = json.dumps(entity_data) if entity_data else '{}'
            cursor.execute(insert_query, (entity_id, data_json, summary))
            conn.commit()
            logger.info(f"Сущность с id {entity_id} добавлена в таблицу {table_name}_entities.")
        else:
            # Если запись существует, обновляем данные и summary
            update_query = f"UPDATE {table_name}_entities SET data = %s, summary = %s WHERE id = %s"
            data_json = json.dumps(entity_data) if entity_data else '{}'
            cursor.execute(update_query, (data_json, summary, entity_id))
            conn.commit()
            logger.info(f"Сущность с id {entity_id} обновлена в таблице {table_name}_entities.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при проверке/добавлении сущности: {e}")
    finally:
        cursor.close()
        conn.close()


# Пример вызова
if __name__ == "__main__":
    table = "advertpro"
    file_name = "2025-01-21 10-34-00 +79166388987.mp3"
    metadata = {
        'encoding': 'MPEG_AUDIO',
        'num_channels': 2,
        'sample_rate_hertz': 8000,
        'duration': 274.82
    }
    uri = "storage://s3.api.tinkoff.ai/inbound/2025-01-21 10-34-00 +79166388987.mp3"
    call_datetime = datetime.strptime("2025-01-21 10:34:00", "%Y-%m-%d %H:%M:%S")

    # Пример: user_id=1, entity_id=100
    # Сначала проверяем наличие сущности в таблице сущностей
    check_and_insert_entity(table, 100)
    insert_data(
        table_name=table,
        file_name=file_name,
        metadata=metadata,
        uri=uri,
        date=call_datetime,
        user_id=1,
        entity_id=100
    )
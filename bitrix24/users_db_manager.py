import asyncio
import sys
import os

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_db_client
from debug_utils import save_debug_json
from logger_config import setup_logger

logger = setup_logger('users_db_manager', 'logs/users_db_manager.log')


def upsert_users_to_db(portal_name: str, users: list) -> dict:
    """
    Создает или обновляет пользователей в таблице {portal_name}_users.
    
    :param portal_name: Название портала (например, 'advertpro')
    :param users: Список пользователей с полями id, NAME, LAST_NAME, UF_DEPARTMENT
    :return: Словарь {user_id: user_id} (для совместимости)
    """
    if not users:
        logger.info(f"Нет пользователей для обновления в портале {portal_name}")
        return {}
    
    logger.info(f"Обновляю {len(users)} пользователей в БД для портала {portal_name}")
    
    user_id_mapping = {}
    
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            try:
                for user in users:
                    user_id = user.get('id')
                    name = user.get('NAME')
                    last_name = user.get('LAST_NAME')
                    uf_department = user.get('UF_DEPARTMENT')
                    
                    if not user_id:
                        logger.warning(f"Пользователь без ID, пропускаю: {user}")
                        continue
                    
                    # Преобразуем user_id в integer
                    try:
                        user_id_int = int(user_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Некорректный user_id: {user_id}, пропускаю")
                        continue
                    
                    # UPSERT: INSERT ... ON CONFLICT DO UPDATE
                    upsert_query = f"""
                        INSERT INTO {portal_name}_users (id, name, last_name, uf_department)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) 
                        DO UPDATE SET 
                            name = EXCLUDED.name,
                            last_name = EXCLUDED.last_name,
                            uf_department = EXCLUDED.uf_department
                        RETURNING id;
                    """
                    
                    # Преобразуем uf_department в JSON, если это не None
                    uf_department_json = None
                    if uf_department is not None:
                        if isinstance(uf_department, (list, dict)):
                            import json
                            uf_department_json = json.dumps(uf_department)
                        else:
                            uf_department_json = str(uf_department)
                    
                    cursor.execute(upsert_query, (
                        user_id_int,
                        name,
                        last_name,
                        uf_department_json
                    ))
                    
                    # Получаем ID созданной/обновленной записи
                    result = cursor.fetchone()
                    if result:
                        internal_id = result[0]
                        user_id_mapping[user_id] = internal_id
                        logger.debug(f"Пользователь {user_id} ({name} {last_name}) обновлен, ID: {internal_id}")
                
                conn.commit()
                logger.info(f"Успешно обновлено {len(user_id_mapping)} пользователей для портала {portal_name}")
                
            except Exception as e:
                logger.error(f"Ошибка при обновлении пользователей для портала {portal_name}: {e}")
                conn.rollback()
                return {}
    
    return user_id_mapping


async def update_users_in_database(records_dict: dict):
    """
    Обновляет пользователей в БД для всех порталов из словаря.
    Принимает результат после Шага 5, обновляет таблицы {portal}_users.
    
    :param records_dict: Словарь с данными после Шага 5 (содержит ключ 'users')
    """
    logger.info("=== НАЧИНАЮ ОБРАБОТКУ: Обновление пользователей в БД ===")
    logger.info("Начинаю обновление пользователей в БД")
    
    for portal_name, portal_data in records_dict.items():
        logger.info(f"Обрабатываю портал: {portal_name}")
        
        # Получаем список пользователей
        users = portal_data.get('users', [])
        
        if not users:
            logger.info(f"Нет пользователей для портала {portal_name}")
            continue
        
        # Обновляем пользователей в БД
        user_mapping = upsert_users_to_db(portal_name, users)
        
        if user_mapping:
            logger.info(f"Пользователи для портала {portal_name} успешно обновлены в БД")
        else:
            logger.error(f"Не удалось обновить пользователей для портала {portal_name}")
    
    logger.info("Обновление пользователей в БД завершено")
    logger.info("=== ЗАВЕРШАЮ ОБРАБОТКУ: Обновление пользователей в БД ===")


if __name__ == "__main__":
    async def test():
        logger.info("Тестирую обновление пользователей в БД")
        
        # Жестко заданные тестовые данные (как после Шага 5)
        test_records = {
            "advertpro": {
                "records": [
                    {
                        "id": "call_493123",
                        "date": "2025-08-07T10:00:19+03:00",
                        "user_id": "11009",
                        "phone_number": "+79189616367"
                    }
                ],
                "entities": [
                    {
                        "id": 15,
                        "entity_type_id": 3,
                        "entity_id": 27523,
                        "title": "Тестовый контакт"
                    }
                ],
                "users": [
                    {
                        "id": "11009",
                        "NAME": "Иван",
                        "LAST_NAME": "Петров33",
                        "UF_DEPARTMENT": [1, 5, 6]
                    },
                    {
                        "id": "15437",
                        "NAME": "Анна",
                        "LAST_NAME": "Сидорова",
                        "UF_DEPARTMENT": [3]
                    }
                ]
            }
        }
        
        logger.info("Используются жестко заданные тестовые данные")
        
        # Тестируем функцию обновления
        await update_users_in_database(test_records)
        
        logger.info("Проверьте БД для подтверждения обновления пользователей")
    
    asyncio.run(test())
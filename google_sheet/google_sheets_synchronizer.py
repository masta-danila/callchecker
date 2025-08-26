import asyncio
import sys
import os

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google_sheet.db_records_loader import load_records_entities_and_users
from debug_utils import save_debug_json
from logger_config import get_sheets_logger

# Настройка логгера для этого модуля
logger = get_sheets_logger()


async def main(delay: int):
    """
    Асинхронная основная функция, которая в бесконечном цикле:
    1) Загружает данные из БД и синхронизирует с Google таблицами
    2) Ждёт заданное количество секунд перед повторным запуском
    """
    logger.info("Запуск сервиса синхронизации с Google Sheets")
    while True:
        try:
            logger.info("Шаг 1: Получаю данные из БД")
            records = await load_records_entities_and_users()
            logger.info(f"Загружено {len(records)} записей из БД")

            # Сохраняем отладочные данные
            save_debug_json(records, "google_sheets_records")

            logger.info("Шаг 2: Синхронизирую сущности с Google таблицами")
            from google_sheet.entities_uploader import upload_entities_to_google_sheets_all_portals
            await upload_entities_to_google_sheets_all_portals(records)
            logger.info("Сущности успешно синхронизированы")
            
            logger.info("Шаг 3: Синхронизирую записи с Google таблицами")
            from google_sheet.records_uploader import upload_to_google_sheets
            await upload_to_google_sheets(records)
            logger.info("Записи успешно синхронизированы")
            
            logger.info("Все шаги успешно выполнены! Ожидаю перед следующим циклом...")
            await asyncio.sleep(delay)
            
        except Exception as e:
            logger.error(f"Ошибка при синхронизации с Google Sheets: {e}")
            await asyncio.sleep(60)  # Ждем минуту перед повтором


if __name__ == "__main__":
    asyncio.run(main(delay=300))

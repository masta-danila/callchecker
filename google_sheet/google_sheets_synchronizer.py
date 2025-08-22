import asyncio
import sys
import os

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google_sheet.db_records_loader import load_records_entities_and_users
from debug_utils import save_debug_json


async def main(delay: int):
    """
    Асинхронная основная функция, которая в бесконечном цикле:
    1) Загружает данные из БД и синхронизирует с Google таблицами
    2) Ждёт заданное количество секунд перед повторным запуском
    """
    while True:
        print("Шаг 1: Получаю данные из БД")
        records = await load_records_entities_and_users()

        # Сохраняем отладочные данные
        save_debug_json(records, "google_sheets_records")

        print("Шаг 2: Синхронизирую записи с Google таблицами")
        from google_sheet.records_uploader import upload_to_google_sheets
        await upload_to_google_sheets(records)
        
        print("Шаг 3: Синхронизирую сущности с Google таблицами")
        from google_sheet.entities_uploader import upload_entities_to_google_sheets_all_portals
        await upload_entities_to_google_sheets_all_portals(records)
        
        print("Все шаги успешно выполнены! Ожидаю перед следующим циклом...")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(main(delay=300))

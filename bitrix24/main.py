import asyncio
import json
import os
import sys

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from audio_cleanup import cleanup_audio_files_after_db_upload
from audio_processor import process_audio_files_async
from bulk_call_downloader import download_missing_calls_from_records
from call_downloader import download_call_by_id
from db_fetcher import fetch_data_with_portal_settings
from debug_utils import save_debug_json
from entity_db_manager import update_entities_in_database
from entity_fetcher import fetch_entities_for_records
from logger_config import get_main_logger
from records_db_manager import update_records_in_database
from users_db_manager import update_users_in_database
from users_fetcher import add_users_to_records

# Настройка логгера для этого модуля
logger = get_main_logger()


async def main(delay: int):
    """
    Асинхронная основная функция, которая в бесконечном цикле:
    1) Выполняет полный цикл обработки звонков из Bitrix24
    2) Ждёт заданное количество секунд перед повторным запуском
    """
    while True:
        logger.info("Начинаю процесс скачивания и загрузки файлов с индивидуальными настройками для каждого портала")
        
        # Шаг 1: Получаю все записи из БД с индивидуальными настройками для каждого портала
        logger.info("Шаг 1: Получаю все записи из БД с индивидуальными настройками для каждого портала")
        records = fetch_data_with_portal_settings(
            status=None,
            fields=["id"],
            analytics_mode=False
        )

        # Сохраняем отладочные данные
        save_debug_json(records, "records")
        
        # Шаг 2: Скачиваем недостающие файлы
        logger.info("Шаг 2: Скачиваю недостающие файлы")
        downloaded_records = await download_missing_calls_from_records(
            records,
            max_concurrent_requests=50,
            request_delay=0.1,
            retries=3
        )
        
        # Сохраняем отладочные данные
        save_debug_json(downloaded_records, "downloaded_records")
        
        # Шаг 3: Получаем данные сущностей из Bitrix24
        logger.info("Шаг 3: Получаю данные сущностей из Bitrix24")
        enhanced_records = await fetch_entities_for_records(
            downloaded_records,
            max_concurrent_requests=50,
            request_delay=0.1,
            retries=3
        )
        
        # Сохраняем отладочные данные
        save_debug_json(enhanced_records, "enhanced_records")
        
        # Шаг 4: Обновляем сущности в БД и получаем id сущностей
        logger.info("Шаг 4: Обновляю сущности в БД")
        final_records = await update_entities_in_database(enhanced_records)
        
        # Сохраняем финальные данные
        save_debug_json(final_records, "final_records")
        
        # Шаг 5: Получаем данные о пользователях
        logger.info("Шаг 5: Получаю данные о пользователях")
        complete_records = await add_users_to_records(
            final_records,
            max_concurrent_portals=2,
            request_delay=0.1,
            retries=3
        )
        
        # Сохраняем полные данные
        save_debug_json(complete_records, "complete_records")
        
        # Шаг 6: Обновляем пользователей в БД
        logger.info("Шаг 6: Обновляю пользователей в БД")
        await update_users_in_database(complete_records)
        
        # Шаг 7: Получаем аудио метаданные и выгружаем файлы в облако
        logger.info("Шаг 7: Получаю аудио метаданные и выгружаю файлы в облако")
        processed_records = await process_audio_files_async(
            complete_records,
            max_concurrent_files=50,
            retries=3,
            retry_delay=1.0
        )
        
        # Сохраняем финальные данные с аудио метаданными
        save_debug_json(processed_records, "processed_records")
        
        # Шаг 8: Загружаем записи в БД
        logger.info("Шаг 8: Загружаю записи в БД")
        final_result = await update_records_in_database(processed_records)
        
        # Сохраняем финальный результат
        save_debug_json(final_result, "final_result")
        
        # Шаг 9: Очищаем аудиофайлы после успешной загрузки в БД
        logger.info("Шаг 9: Очищаю папки с аудиофайлами после успешной загрузки в БД")
        await cleanup_audio_files_after_db_upload(final_result)
        
        logger.info("Все шаги успешно выполнены! Ожидаю перед следующим циклом...")
        await asyncio.sleep(delay)
    

if __name__ == "__main__":
    asyncio.run(main(delay=600))
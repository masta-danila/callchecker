import asyncio
import time
from db_fetcher import fetch_data
from dialog_processor import process_and_store_dialogs
from db_dialog_uploader import upload_recognized_dialogs
from logger_config import get_dialogue_logger

# Настройка логгера для этого модуля
logger = get_dialogue_logger()


async def process_data():
    logger.info("Загрузка данных из базы...")
    try:
        data_records = fetch_data(
            status="uploaded",
            fields=["id", "dialogue", "audio_metadata", "status"],
            analytics_mode=False
        )

        if not data_records:
            logger.info("Нет загруженных файлов для распознавания.")
            return

        logger.info(f"Найдено {len(data_records)} файлов для распознавания")
        logger.info("Начинается распознавание аудиофайлов...")
        
        updated_data_records = await process_and_store_dialogs(
            data_records,
            max_concurrent_requests=50,
            retries=3,
            request_delay=1
        )
        logger.info(f"Распознано {len(updated_data_records)} диалогов")

        logger.info("Загрузка распознанных данных обратно в базу...")
        upload_recognized_dialogs(
            updated_data_records,
            default_status='recognized'  # Fallback статус, если в записи нет поля 'status'
        )
        logger.info("Распознанные данные успешно загружены в БД")

        logger.info("Распознавание завершено!")
        
    except Exception as e:
        logger.error(f"Ошибка при распознавании диалогов: {e}")
        raise


async def main(sleep_time: int):
    """Основной цикл, вызывающий process_data в бесконечном цикле с задержкой."""
    logger.info("Запуск сервиса распознавания диалогов")
    while True:
        try:
            await process_data()
            logger.info(f"Ждём {sleep_time} секунд перед следующей итерацией...")
            await asyncio.sleep(sleep_time)
        except Exception as e:
            logger.error(f"Критическая ошибка в основном цикле: {e}")
            await asyncio.sleep(30)  # Ждем 30 секунд перед повтором


if __name__ == "__main__":
    # Можно запускать с разным временем задержки.
    asyncio.run(main(sleep_time=600))

import asyncio
import time
from db_fetcher import fetch_data
from dialog_processor import process_and_store_dialogs
from db_dialog_uploader import upload_recognized_dialogs


async def process_data():
    print("Загрузка данных из базы...")
    data_records = fetch_data(
        status="uploaded",
        fields=["id", "dialogue", "audio_metadata", "status"],
        analytics_mode=False
    )

    if not data_records:
        print("Нет загруженных файлов для распознавания.")
        return

    print("Начинается распознавание аудиофайлов...")
    updated_data_records = await process_and_store_dialogs(
        data_records,
        max_concurrent_requests=50,
        retries=3,
        request_delay=1
    )

    print("Загрузка распознанных данных обратно в базу...")
    upload_recognized_dialogs(
        updated_data_records,
        default_status='recognized'  # Fallback статус, если в записи нет поля 'status'
    )

    print("Распознавание завершено!")


async def main(sleep_time: int):
    """Основной цикл, вызывающий process_data в бесконечном цикле с задержкой."""
    while True:
        await process_data()
        print(f"Ждём {sleep_time} секунд перед следующей итерацией...")
        await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    # Можно запускать с разным временем задержки.
    asyncio.run(main(sleep_time=60))

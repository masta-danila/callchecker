import json
import asyncio
from db_fetcher import fetch_data
from dialog_fixer_all import process_dialogs
from db_dialog_uploader import upload_recognized_dialogs
from dialog_classifier import classify_dialogs
from criteria_analyzer import analyze_criteria
from db_data_uploader import upload_records_from_dict


async def main(delay: int):
    """
    Асинхронная основная функция, которая в бесконечном цикле:
    1) Обрабатывает диалоги и обновляет данные в БД.
    2) Ждёт заданное количество секунд перед повторным запуском.
    """
    while True:
        print("Шаг 1: Получаю сырые диалоги из БД")
        records = fetch_data(
            status="recognized",
            fields=["id", "dialogue"],
            analytics_mode=False
        )

        print("Шаг 2: Обрабатываю тексты диалогов")
        processed_records = await process_dialogs(
            records,
            max_concurrent_requests=50,
            request_delay=0.1,
            retries=3
        )

        print("Шаг 3: Загружаю обработанные диалоги обратно в БД")
        upload_recognized_dialogs(processed_records, status='fixed')

        print("Шаг 4: Получаю диалоги для анализа из БД")
        fixed_records = fetch_data(
            status="fixed",
            fields=["id", "dialogue", "data", "user_id", "entity_id"],
            analytics_mode=True
        )

        print("Шаг 5: Классифицирую диалоги")
        classified_records = await classify_dialogs(
            fixed_records,
            max_concurrent_requests=50,
            retry_delay=0.1,
            max_retries=3
        )
        # print(json.dumps(classified_records, indent=4, ensure_ascii=False))

        print("Шаг 7: Провожу анализ диалогов согласно критериям")
        final_records = await analyze_criteria(
            classified_records,
            max_concurrent_requests=100,
            retry_delay=0.1,
            max_retries=3
        )
        print(json.dumps(final_records, indent=4, ensure_ascii=False))
        #
        # print("Шаг 8: Загружаю итоговые данные в БД")
        # upload_records_from_dict(final_records, status='ready')
        #
        print("Все шаги успешно выполнены! Ожидаю перед следующим циклом...")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(main(delay=120))

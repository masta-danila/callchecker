import asyncio
from db_fetcher import fetch_data
from dialog_fixer_all import process_dialogs
from db_dialog_uploader import upload_recognized_dialogs
from dialog_classifier import classify_dialogs
from criteria_analyzer import analyze_criteria
from db_data_uploader import upload_full_data_from_dict
from debug_utils import save_debug_json, convert_datetime_to_string
from entity_summarizer import summarize_entity_descriptions
from entity_summary_aggregator import aggregate_entity_summaries


async def main(delay: int):
    """
    Асинхронная основная функция, которая в бесконечном цикле:
    1) Обрабатывает диалоги и обновляет данные в БД.
    2) Суммирует данные по критериям сущностей.
    3) Агрегирует summary сущностей.
    4) Загружает итоговые данные в БД.
    5) Ждёт заданное количество секунд перед повторным запуском.
    """
    while True:
        print("Шаг 1: Получаю сырые диалоги из БД")
        records = fetch_data(
            status="recognized",
            fields=["id", "dialogue", "status"],
            analytics_mode=False
        )

        # Сохраняем отладочные данные
        save_debug_json(records, "records")

        print("Шаг 2: Исправляю тексты диалогов и получаю резюме диалогов")
        processed_records = await process_dialogs(
            records,
            max_concurrent_requests=50,
            request_delay=0.1,
            retries=3
        )

        # Сохраняем отладочные данные
        save_debug_json(processed_records, "processed_records")
        
        print("Шаг 3: Загружаю обработанные диалоги обратно в БД")
        upload_recognized_dialogs(processed_records, default_status='fixed')

        print("Шаг 4: Получаю диалоги для анализа из БД")
        fixed_records = fetch_data(
            status="fixed",
            fields=["id", "dialogue", "data", "user_id", "entity_id", "date" , "summary"],
            analytics_mode=True
        )

        # Сохраняем отладочные данные
        save_debug_json(fixed_records, "fixed_records")

        print("Шаг 5: Классифицирую диалоги")
        classified_records = await classify_dialogs(
            fixed_records,
            max_concurrent_requests=50,
            retry_delay=0.1,
            max_retries=3
        )

        # Сохраняем отладочные данные
        save_debug_json(classified_records, "classified_records") 

        print("Шаг 6: Провожу анализ диалогов согласно критериям")
        analyzed_records = await analyze_criteria(
            classified_records,
            max_concurrent_requests=500,
            retry_delay=0.1,
            max_retries=3
        )


        # Сохраняем отладочные данные
        save_debug_json(analyzed_records, "analyzed_records")
        
        print("Шаг 7: Суммирую данные сущностей")
        final_records = await summarize_entity_descriptions(
            analyzed_records,
            max_text_size=1000,
            max_concurrent_requests=50,
            request_delay=0.1,
            retries=3
        )

        # Сохраняем отладочные данные
        save_debug_json(final_records, "final_records")
        
        print("Шаг 8: Суммирую summary сущностей")
        entities_with_aggregated_summaries = await aggregate_entity_summaries(
            final_records,
            max_text_size=1000,
            max_concurrent_requests=50,
            request_delay=0.1,
            retries=3
        )

        # Сохраняем отладочные данные
        save_debug_json(entities_with_aggregated_summaries, "entities_with_aggregated_summaries")
        
        print("Шаг 9: Загружаю итоговые данные в БД")
        upload_full_data_from_dict(entities_with_aggregated_summaries, status='ready')
        
        print("Все шаги успешно выполнены! Ожидаю перед следующим циклом...")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(main(delay=120))

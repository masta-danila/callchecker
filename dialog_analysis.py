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
from logger_config import get_analysis_logger

# Настройка логгера для этого модуля
logger = get_analysis_logger()


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
        logger.info("Шаг 1: Получаю сырые диалоги из БД")
        try:
            records = fetch_data(
                status="recognized",
                fields=["id", "dialogue", "status"],
                analytics_mode=False
            )
            logger.info(f"Получено {len(records)} диалогов для обработки")

            # Сохраняем отладочные данные
            save_debug_json(records, "records")

            logger.info("Шаг 2: Исправляю тексты диалогов и получаю резюме диалогов")
            processed_records = await process_dialogs(
                records,
                max_concurrent_requests=500,
                request_delay=0.1,
                retries=3
            )
            logger.info(f"Обработано {len(processed_records)} диалогов")

            # Сохраняем отладочные данные
            save_debug_json(processed_records, "processed_records")
            
            logger.info("Шаг 3: Загружаю обработанные диалоги обратно в БД")
            upload_recognized_dialogs(processed_records, default_status='fixed')
            logger.info("Диалоги успешно загружены в БД")
        except Exception as e:
            logger.error(f"Ошибка на шагах 1-3: {e}")
            continue

        logger.info("Шаг 4: Получаю диалоги для анализа из БД")
        try:
            fixed_records = fetch_data(
                status="fixed",
                fields=["id", "dialogue", "data", "user_id", "entity_id", "date" , "summary"],
                analytics_mode=True
            )
            logger.info(f"Получено {len(fixed_records)} диалогов для анализа")

            # Сохраняем отладочные данные
            save_debug_json(fixed_records, "fixed_records")

            logger.info("Шаг 5: Классифицирую диалоги")
            classified_records = await classify_dialogs(
                fixed_records,
                max_concurrent_requests=500,
                retry_delay=0.1,
                max_retries=3
            )
            logger.info(f"Классифицировано {len(classified_records)} диалогов")

            # Сохраняем отладочные данные
            save_debug_json(classified_records, "classified_records") 

            logger.info("Шаг 6: Провожу анализ диалогов согласно критериям")
            analyzed_records = await analyze_criteria(
                classified_records,
                max_concurrent_requests=500,
                retry_delay=0.1,
                max_retries=3
            )
            logger.info(f"Проанализировано {len(analyzed_records)} диалогов")
        except Exception as e:
            logger.error(f"Ошибка на шагах 4-6: {e}")
            continue


        # Сохраняем отладочные данные
        save_debug_json(analyzed_records, "analyzed_records")
        
        logger.info("Шаг 7: Суммирую данные сущностей")
        try:
            final_records = await summarize_entity_descriptions(
                analyzed_records,
                max_text_size=1000,
                max_concurrent_requests=500,
                request_delay=0.1,
                retries=3
            )
            logger.info("Данные сущностей успешно суммированы")

            # Сохраняем отладочные данные
            save_debug_json(final_records, "final_records")
            
            logger.info("Шаг 8: Суммирую summary сущностей")
            entities_with_aggregated_summaries = await aggregate_entity_summaries(
                final_records,
                max_text_size=1000,
                max_concurrent_requests=500,
                request_delay=0.1,
                retries=3
            )
            logger.info("Summary сущностей успешно агрегированы")

            # Сохраняем отладочные данные
            save_debug_json(entities_with_aggregated_summaries, "entities_with_aggregated_summaries")
            
            logger.info("Шаг 9: Загружаю итоговые данные в БД")
        except Exception as e:
            logger.error(f"Ошибка на шагах 7-8: {e}")
            continue
            
        try:
            upload_full_data_from_dict(entities_with_aggregated_summaries, status='ready')
            logger.info("Итоговые данные успешно загружены в БД")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных в БД: {e}")
            continue
        
        logger.info("Все шаги успешно выполнены! Ожидаю перед следующим циклом...")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(main(delay=120))

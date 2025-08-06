import asyncio
from dialogue_recognizer import process_record  # Импорт функции из record_processor.py


async def process_and_store_dialogs(
    data: dict,
    max_concurrent_requests: int,
    retries: int,
    request_delay: float
):
    """
    Обрабатывает записи параллельно с ограничением по количеству одновременно
    обрабатываемых записей. В случае ошибки предпримет заданное количество
    повторных попыток с задержкой между ними.

    :param data: Словарь вида {category1: {"records": [record1, record2, ...]}, category2: {"records": [...]}, ...}
    :param max_concurrent_requests: максимальное количество одновременно обрабатываемых записей
    :param retries: количество повторных попыток при возникновении ошибки
    :param request_delay: задержка (в секундах) между повторными попытками
    """
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def sem_protected_process_record(record):
        """Вспомогательная функция для ограничения параллелизма и повторных попыток."""
        async with semaphore:
            for attempt in range(retries):
                try:
                    return await process_record(record)
                except Exception as e:
                    if attempt < retries - 1:
                        print(
                            f"Ошибка при распознавании записи (id={record.get('id', 'unknown')}), "
                            f"попытка {attempt + 1} из {retries}: {e}"
                        )
                        await asyncio.sleep(request_delay)
                    else:
                        print(
                            f"Не удалось распознать запись (id={record.get('id', 'unknown')}) "
                            f"после {retries} попыток. Ошибка: {e}"
                        )
                        return None

    # Подготовим список (category, record) для удобства.
    # Предполагается, что записи для каждой категории находятся в ключе "records"
    all_records = []
    for category, category_data in data.items():
        records = category_data.get("records", [])
        for record in records:
            all_records.append((category, record))

    # Запустим все задачи параллельно
    tasks = [sem_protected_process_record(record) for _, record in all_records]
    results = await asyncio.gather(*tasks)

    # Собираем только успешные результаты обратно в структуру {category: {"records": [record, ...]}, ...}
    filtered_data = {}
    for (category, _), processed_record in zip(all_records, results):
        if processed_record is not None:
            if category not in filtered_data:
                filtered_data[category] = {"records": []}
            filtered_data[category]["records"].append(processed_record)

    return filtered_data

if __name__ == "__main__":
    from db_fetcher import fetch_data

    data_records = fetch_data(
        status="uploaded",
        fields=["id", "dialogue", "audio_metadata"],
        analytics_mode=False
    )

    # Пример вызова функции с ограничением в 50 одновременных задач,
    # 5 повторными попытками и 2 секундами задержки между ними
    updated_data_records = asyncio.run(
        process_and_store_dialogs(
            data_records,
            max_concurrent_requests=50,
            retries=5,
            request_delay=2
        )
    )

    print("Обработанные записи:", updated_data_records)
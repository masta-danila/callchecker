import asyncio
from sum_texts import sum_text_blocks
from logger_config import setup_logger

# Настройка логгера для этого модуля
logger = setup_logger('entity_summarizer', 'logs/entity_summarizer.log')


async def summarize_entity_descriptions(
    data_dict, 
    max_text_size=1000,
    max_concurrent_requests=50,
    request_delay=0.1,
    retries=3
):
    """
    Проходит по всем сущностям в словаре, находит критерии с "include_in_entity_description": true
    в записях и добавляет/суммирует их в data сущностей.
    
    :param data_dict: Словарь с данными таблиц. Формат:
                      {
                          "table_name": {
                              "records": [список записей],
                              "criteria": [список определений критериев],
                              "categories": [список категорий]
                          }
                      }
    :param max_text_size: Максимальный размер суммированного текста в словах (по умолчанию 1000)
    :param max_concurrent_requests: Максимальное количество параллельных запросов к LLM
    :param request_delay: Задержка между запросами в секундах
    :param retries: Количество повторных попыток при ошибке
    :return: Обновленный словарь с суммированными описаниями сущностей
    """
    
    # Создаем копию ключей для безопасной итерации
    table_names = list(data_dict.keys())
    
    # Список задач для параллельного выполнения
    tasks = []
    
    for table_name in table_names:
        table_data = data_dict.get(table_name, {})
        records = table_data.get("records", [])
        criteria_definitions = table_data.get("criteria", [])
        
        # Создаем маппинг entity_id -> список записей для этой сущности
        entity_records = {}
        for record in records:
            entity_id = record.get("entity_id")
            if entity_id is not None:
                if entity_id not in entity_records:
                    entity_records[entity_id] = []
                entity_records[entity_id].append(record)
        
        # Обрабатываем каждую сущность
        for entity_id, entity_record_list in entity_records.items():
            # Собираем все критерии из всех записей этой сущности, которые нужно включить в описание
            criteria_to_process = {}
            
            for record in entity_record_list:
                criteria_list = record.get("data", {}).get("criteria", [])
                
                for criterion in criteria_list:
                    criterion_id = criterion.get("id")
                    if criterion_id is None:
                        continue
                    
                    # Находим полное определение критерия
                    full_criterion = next(
                        (c for c in criteria_definitions if c["id"] == criterion_id),
                        None
                    )
                    
                    # Проверяем, нужно ли включать в описание сущности
                    if (full_criterion and 
                        full_criterion.get("include_in_entity_description", False)):
                        
                        if criterion_id not in criteria_to_process:
                            criteria_to_process[criterion_id] = {
                                "definition": full_criterion,
                                "instances": []
                            }
                        
                        # Добавляем экземпляр критерия из записи
                        criteria_to_process[criterion_id]["instances"].append(criterion)
            
            # Если есть критерии для обработки, добавляем задачу в список
            if criteria_to_process:
                logger.info(f"Планирую обработку сущности {entity_id} в таблице {table_name}: {len(criteria_to_process)} критериев")
                tasks.append(_update_entity_data(
                    data_dict, table_name, entity_id, criteria_to_process, 
                    max_text_size, max_concurrent_requests, request_delay, retries
                ))
    
    # Выполняем все задачи параллельно
    if tasks:
        logger.info(f"Запускаю параллельную обработку {len(tasks)} сущностей...")
        await asyncio.gather(*tasks)
        logger.info("Параллельная обработка сущностей завершена!")
    
    return data_dict


async def _update_entity_data(data_dict, table_name, entity_id, criteria_to_process, max_text_size, max_concurrent_requests, request_delay, retries):
    """
    Обновляет данные сущности, суммируя критерии.
    
    :param data_dict: Основной словарь данных
    :param table_name: Название таблицы
    :param entity_id: ID сущности
    :param criteria_to_process: Словарь критериев для обработки
    :param max_text_size: Максимальный размер текста для суммирования
    :param max_concurrent_requests: Максимальное количество параллельных запросов
    :param request_delay: Задержка между запросами в секундах
    :param retries: Количество повторных попыток при ошибке
    """
    
    # Получаем или создаем данные сущности в структуре table_data["entities"]
    table_data = data_dict[table_name]
    
    if "entities" not in table_data:
        table_data["entities"] = []
    
    entities_data = table_data["entities"]
    
    # Находим существующую сущность или создаем новую
    entity_record = None
    for entity in entities_data:
        if entity.get("id") == entity_id:
            entity_record = entity
            break
    
    if entity_record is None:
        entity_record = {"id": entity_id, "data": {}}
        entities_data.append(entity_record)
    
    # Инициализируем data если его нет
    if "data" not in entity_record:
        entity_record["data"] = {}
    
    if "criteria" not in entity_record["data"]:
        entity_record["data"]["criteria"] = []
    
    existing_criteria = entity_record["data"]["criteria"]
    
    # Обрабатываем каждый тип критерия
    for criterion_id, criterion_data in criteria_to_process.items():
        criterion_definition = criterion_data["definition"]
        instances = criterion_data["instances"]
        
        # Ищем существующий критерий в данных сущности
        existing_criterion = None
        for existing in existing_criteria:
            if existing.get("id") == criterion_id:
                existing_criterion = existing
                break
        
        # Собираем все тексты и оценки для суммирования
        text_eval_pairs = []
        
        # Добавляем существующий критерий если есть
        if existing_criterion:
            existing_text = existing_criterion.get("text", "")
            existing_eval = existing_criterion.get("evaluation")
            if existing_text:
                text_eval_pairs.append((existing_text, existing_eval))
        
        # Добавляем все экземпляры из записей
        for instance in instances:
            instance_text = instance.get("text", "")
            instance_eval = instance.get("evaluation")
            if instance_text:
                text_eval_pairs.append((instance_text, instance_eval))
        
        # Если есть что суммировать
        if text_eval_pairs:
            if len(text_eval_pairs) == 1:
                # Если только один текст, просто копируем
                final_text = text_eval_pairs[0][0]
                final_eval = text_eval_pairs[0][1]
            else:
                # Суммируем через LLM с повторными попытками
                for attempt in range(1, retries + 1):
                    try:
                        result = await sum_text_blocks(text_eval_pairs, max_size=max_text_size)
                        final_text = result.get("text_result", "")
                        final_eval = result.get("evaluation_result")
                        logger.debug(f"[OK]   Entity={entity_id}, критерий={criterion_definition.get('name')!r}, попытка={attempt}")
                        break
                    except Exception as e:
                        logger.warning(f"[ERR]  Entity={entity_id}, критерий={criterion_definition.get('name')!r}, попытка={attempt}: {e}")
                        if attempt < retries:
                            await asyncio.sleep(request_delay)
                        else:
                            logger.error(f"[FAIL] Entity={entity_id}, критерий={criterion_definition.get('name')!r} — исчерпаны все {retries} попыток")
                            final_text = ""
                            final_eval = None
            
            # Создаем итоговый критерий
            final_criterion = {
                "id": criterion_id,
                "name": criterion_definition.get("name", ""),
                "text": final_text,
                "evaluation": final_eval
            }
            
            # Обновляем или добавляем критерий
            if existing_criterion:
                # Обновляем существующий
                existing_criterion.update(final_criterion)
                logger.info(f"Обновлен критерий {criterion_definition.get('name')!r} для сущности {entity_id}")
            else:
                # Добавляем новый
                existing_criteria.append(final_criterion)
                logger.info(f"Добавлен критерий {criterion_definition.get('name')!r} для сущности {entity_id}")


# Пример использования
if __name__ == "__main__":
    async def main():
        # Загружаем реальные данные из json_tests/analyzed_records.json
        import json
        
        with open('json_tests/analyzed_records.json', 'r', encoding='utf-8') as f:
            test_data = json.load(f)
        
        logger.info("Загружены данные из analyzed_records.json:")
        for table_name, table_data in test_data.items():
            records_count = len(table_data.get("records", []))
            entities_count = len(table_data.get("entities", []))
            logger.info(f"  {table_name}: {records_count} records, {entities_count} entities")
        
        logger.info("=== ТЕСТ: Суммирование критериев сущностей ===")
        
        result = await summarize_entity_descriptions(test_data)
        
        logger.info("\n=== РЕЗУЛЬТАТ ===")
        logger.info("Обработка завершена успешно!")

    asyncio.run(main())
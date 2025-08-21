import asyncio
from sum_texts import sum_text_blocks


async def aggregate_entity_summaries(
    data_dict, 
    max_text_size=1000,
    max_concurrent_requests=50,
    request_delay=0.1,
    retries=3
):
    """
    Агрегирует поле summary для сущностей, суммируя summary из связанных записей 
    и существующие summary самих сущностей через LLM.
    
    :param data_dict: Словарь с данными таблиц. Формат:
                      {
                          "table_name": {
                              "records": [список записей с полем summary],
                              "entities": [список сущностей с полем summary],
                              "criteria": [список определений критериев],
                              "categories": [список категорий]
                          }
                      }
    :param max_text_size: Максимальный размер итогового summary в словах
    :param max_concurrent_requests: Максимальное количество параллельных запросов к LLM
    :param request_delay: Задержка между запросами в секундах
    :param retries: Количество повторных попыток при ошибке
    :return: Обновленный словарь с агрегированными summary для сущностей
    """
    
    # Создаем копию ключей для безопасной итерации
    table_names = list(data_dict.keys())
    
    # Список задач для параллельного выполнения
    tasks = []
    
    for table_name in table_names:
        table_data = data_dict.get(table_name, {})
        records = table_data.get("records", [])
        entities = table_data.get("entities", [])
        
        # Создаем маппинг entity_id -> список записей для этой сущности
        entity_records = {}
        for record in records:
            entity_id = record.get("entity_id")
            if entity_id is not None:
                if entity_id not in entity_records:
                    entity_records[entity_id] = []
                entity_records[entity_id].append(record)
        
        # Обрабатываем каждую сущность
        for entity in entities:
            entity_id = entity.get("id")
            if entity_id is None:
                continue
                
            # Собираем все summary для агрегации
            summaries_to_aggregate = []
            
            # Добавляем существующий summary сущности (если есть)
            entity_summary = entity.get("summary", "")
            if entity_summary and entity_summary.strip():
                summaries_to_aggregate.append(entity_summary.strip())
            
            # Добавляем summary из связанных записей
            related_records = entity_records.get(entity_id, [])
            for record in related_records:
                record_summary = record.get("summary", "")
                if record_summary and record_summary.strip():
                    summaries_to_aggregate.append(record_summary.strip())
            
            # Если есть что агрегировать, добавляем задачу
            if len(summaries_to_aggregate) > 1:  # Только если больше одного summary
                print(f"Планирую агрегацию summary для сущности {entity_id} в таблице {table_name}: {len(summaries_to_aggregate)} summary")
                tasks.append(_update_entity_summary(
                    entity, summaries_to_aggregate, 
                    max_text_size, retries, request_delay
                ))
            elif len(summaries_to_aggregate) == 1:
                # Если только один summary, просто копируем его
                entity["summary"] = summaries_to_aggregate[0]
                print(f"Копирую единственный summary для сущности {entity_id}")
    
    # Выполняем все задачи параллельно
    if tasks:
        print(f"Запускаю параллельную агрегацию summary для {len(tasks)} сущностей...")
        await asyncio.gather(*tasks)
        print("Параллельная агрегация summary завершена!")
    
    return data_dict


async def _update_entity_summary(entity, summaries_to_aggregate, max_text_size, retries, request_delay):
    """
    Обновляет summary сущности, агрегируя множественные summary через LLM.
    
    :param entity: Словарь сущности для обновления
    :param summaries_to_aggregate: Список summary для агрегации
    :param max_text_size: Максимальный размер текста для суммирования
    :param retries: Количество повторных попыток при ошибке
    :param request_delay: Задержка между запросами в секундах
    """
    
    entity_id = entity.get("id")
    
    if len(summaries_to_aggregate) <= 1:
        return
    
    # Преобразуем summary в формат для sum_text_blocks (без оценок)
    text_eval_pairs = [(summary, None) for summary in summaries_to_aggregate]
    
    # Суммируем через LLM с повторными попытками
    for attempt in range(1, retries + 1):
        try:
            result = await sum_text_blocks(text_eval_pairs, max_size=max_text_size)
            final_summary = result.get("text_result", "")
            
            # Обновляем summary сущности
            entity["summary"] = final_summary
            print(f"[OK]   Entity={entity_id}, агрегация summary, попытка={attempt}")
            break
            
        except Exception as e:
            print(f"[ERR]  Entity={entity_id}, агрегация summary, попытка={attempt}: {e}")
            if attempt < retries:
                await asyncio.sleep(request_delay)
            else:
                print(f"[FAIL] Entity={entity_id}, агрегация summary — исчерпаны все {retries} попыток")
                # В случае неудачи оставляем первый summary или пустую строку
                entity["summary"] = summaries_to_aggregate[0] if summaries_to_aggregate else ""


# Пример использования
if __name__ == "__main__":
    async def main():
        # Тестовые данные
        test_data = {
            "advertpro": {
                "records": [
                    {
                        "id": "call_494205",
                        "entity_id": 23,
                        "summary": "Клиент сообщил о проблеме с номером, который не отвечает."
                    },
                    {
                        "id": "call_494225", 
                        "entity_id": 23,
                        "summary": "Обсуждение технических вопросов сайта и планирование работ."
                    },
                    {
                        "id": "call_494250",
                        "entity_id": 25,
                        "summary": "Консультация по выбору оборудования для офиса."
                    }
                ],
                "entities": [
                    {
                        "id": 23,
                        "name": "ООО Тест",
                        "summary": "Клиент с долгосрочными проектами по разработке сайтов."
                    },
                    {
                        "id": 25, 
                        "name": "ИП Иванов",
                        "summary": ""  # Пустой summary
                    }
                ],
                "criteria": [],
                "categories": []
            }
        }
        
        print("=== ТЕСТ: Агрегация summary сущностей ===")
        
        result = await aggregate_entity_summaries(test_data)
        
        print("\n=== РЕЗУЛЬТАТ ===")
        for entity in result["advertpro"]["entities"]:
            print(f"Entity {entity['id']}: {entity.get('summary', 'НЕТ SUMMARY')}")

    asyncio.run(main())

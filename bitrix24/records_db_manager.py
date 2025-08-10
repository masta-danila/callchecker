import asyncio
import json
import sys
import os

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_db_client


def upsert_records_to_db(portal_name: str, records: list) -> list:
    """
    Выполняет UPSERT операции для записей звонков в таблице портала.
    
    :param portal_name: Имя портала
    :param records: Список записей для вставки/обновления
    :return: Список записей с обновленными данными из БД
    """
    if not records:
        print(f"Нет записей для обновления в портале {portal_name}")
        return []
    
    conn = get_db_client()
    cursor = conn.cursor()
    
    table_name = f"{portal_name}"
    
    try:
        updated_records = []
        
        for record in records:
            # Извлекаем аудио метаданные
            audio_metadata = record.get('audio_metadata', {})
            
            # Подготавливаем данные для вставки
            call_id = record.get('id')
            call_date = record.get('date')
            user_id = record.get('user_id')
            phone_number = record.get('phone_number')
            call_type = record.get('call_type')
            
            # Ищем внутренний ID сущности по паре (entity_id, crm_entity_type)
            internal_entity_id = None
            bitrix_entity_id = record.get('entity_id')
            crm_entity_type = record.get('crm_entity_type')
            
            if bitrix_entity_id and crm_entity_type:
                # Ищем запись в таблице entities
                entity_query = f"""
                    SELECT id FROM {table_name}_entities 
                    WHERE entity_id = %s AND crm_entity_type = %s
                """
                cursor.execute(entity_query, (bitrix_entity_id, crm_entity_type))
                entity_result = cursor.fetchone()
                if entity_result:
                    internal_entity_id = entity_result[0]
                    print(f"Найдена сущность: {crm_entity_type} {bitrix_entity_id} -> внутренний ID {internal_entity_id}")
                else:
                    print(f"Сущность не найдена: {crm_entity_type} {bitrix_entity_id}")
            
            # Преобразуем audio_metadata в JSON строку
            audio_metadata_json = json.dumps(audio_metadata) if audio_metadata else None
            
            print(f"Обновляю запись {call_id} в таблице {table_name}")
            
            # UPSERT запрос - устанавливаем статус 'uploaded'
            query = f"""
                INSERT INTO {table_name} (
                    id, date, user_id, phone_number, entity_id, call_type, audio_metadata, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, 'uploaded'
                )
                ON CONFLICT (id) DO UPDATE SET
                    date = EXCLUDED.date,
                    user_id = EXCLUDED.user_id,
                    phone_number = EXCLUDED.phone_number,
                    entity_id = EXCLUDED.entity_id,
                    call_type = EXCLUDED.call_type,
                    audio_metadata = EXCLUDED.audio_metadata,
                    status = 'uploaded'
                RETURNING id;
            """
            
            cursor.execute(query, (
                call_id, call_date, user_id, phone_number, internal_entity_id, call_type, audio_metadata_json
            ))
            
            result = cursor.fetchone()
            if result:
                print(f"Запись {call_id} успешно обновлена в БД")
                updated_records.append(record)
            else:
                print(f"Ошибка при обновлении записи {call_id}")
        
        conn.commit()
        print(f"Портал {portal_name}: {len(updated_records)}/{len(records)} записей успешно обновлено")
        return updated_records
        
    except Exception as e:
        print(f"Ошибка при обновлении записей портала {portal_name}: {e}")
        conn.rollback()
        return []
        
    finally:
        cursor.close()
        conn.close()


async def update_records_in_database(data_dict: dict) -> dict:
    """
    Обновляет записи звонков в БД для всех порталов из словаря.
    
    :param data_dict: Словарь с данными по порталам из предыдущих шагов
    :return: Словарь только со статистикой обновления по порталам
    """
    print("Начинаю обновление записей в БД")
    
    result_dict = {}
    
    for portal_name, portal_data in data_dict.items():
        print(f"\nОбрабатываю портал: {portal_name}")
        
        records = portal_data.get('records', [])
        if not records:
            print(f"Нет записей для портала {portal_name}")
            result_dict[portal_name] = {
                'db_update_stats': {
                    'total_records': 0,
                    'updated_records': 0,
                    'success_rate': 0
                }
            }
            continue
        
        # Обновляем записи в БД
        updated_records = upsert_records_to_db(portal_name, records)
        
        # Сохраняем только статистику
        result_dict[portal_name] = {
            'db_update_stats': {
                'total_records': len(records),
                'updated_records': len(updated_records),
                'success_rate': len(updated_records) / len(records) if records else 0
            }
        }
    
    print("Обновление записей в БД завершено")
    return result_dict


if __name__ == "__main__":
    async def test():
        
        # Тестовые данные - только 2 записи
        test_data = {
            "advertpro": {
                "records": [
            {
                "id": "493125",
                "date": "2025-08-07T10:01:19+03:00",
                "user_id": "15437",
                "phone_number": "+79309620098",
                "entity_id": "27533",
                "call_type": "1",
                "crm_entity_type": "CONTACT",
                "audio_metadata": {
                    "encoding": "MPEG_AUDIO",
                    "num_channels": 2,
                    "sample_rate_hertz": 8000,
                    "duration": 3.46,
                    "uri": "storage://s3.api.tinkoff.ai/inbound/493125.mp3"
                }
            },
            {
                "id": "493129",
                "date": "2025-08-07T10:29:33+03:00",
                "user_id": "11009",
                "phone_number": "+78612583661",
                "entity_id": "347831",
                "call_type": "1",
                "crm_entity_type": "LEAD",
                "audio_metadata": {
                    "encoding": "MPEG_AUDIO",
                    "num_channels": 2,
                    "sample_rate_hertz": 8000,
                    "duration": 6.26,
                    "uri": "storage://s3.api.tinkoff.ai/inbound/493129.mp3"
                }
            },
            {
                "id": "493135",
                "date": "2025-08-07T10:32:19+03:00",
                "user_id": "11009",
                "phone_number": "+79054856753",
                "entity_id": "347833",
                "call_type": "1",
                "crm_entity_type": "LEAD",
                "audio_metadata": {
                    "encoding": "MPEG_AUDIO",
                    "num_channels": 2,
                    "sample_rate_hertz": 8000,
                    "duration": 22.18,
                    "uri": "storage://s3.api.tinkoff.ai/inbound/493135.mp3"
                }
            },
            {
                "id": "493139",
                "date": "2025-08-07T10:47:41+03:00",
                "user_id": "13961",
                "phone_number": "+79851440001",
                "entity_id": "23797",
                "call_type": "1",
                "crm_entity_type": "CONTACT",
                "audio_metadata": {
                    "encoding": "MPEG_AUDIO",
                    "num_channels": 2,
                    "sample_rate_hertz": 8000,
                    "duration": 156.46,
                    "uri": "storage://s3.api.tinkoff.ai/inbound/493139.mp3"
                }
            }
                ],
                "entities": [],
                "users": []
            }
        }
        
        result = await update_records_in_database(test_data)
        
        from debug_utils import save_debug_json
        save_debug_json(result, "db_update_test")
    
    asyncio.run(test())

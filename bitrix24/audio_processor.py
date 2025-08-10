import asyncio
import os
import sys

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from audio_metadata import get_audio_metadata
from upload import upload_file_to_storage_async


async def process_single_audio_file(file_path: str, retries: int = 3, retry_delay: float = 1.0) -> dict:
    """
    Асинхронная обработка одного аудио файла: получение метаданных и загрузка в облако.
    
    :param file_path: Путь к аудио файлу
    :param retries: Количество попыток при ошибках
    :param retry_delay: Задержка между попытками в секундах
    :return: Словарь с audio_metadata
    """
    try:
        print(f"Обрабатываю файл: {os.path.basename(file_path)}")
        
        # Проверяем существование файла
        if not os.path.exists(file_path):
            print(f"Файл не найден: {file_path}")
            return {"error": f"Файл не найден: {file_path}"}
        
        # Получаем метаданные аудио
        metadata = get_audio_metadata(file_path)
        
        if "error" in metadata:
            print(f"Ошибка получения метаданных: {metadata['error']}")
            return metadata
        
        # Загружаем файл в облако с повторными попытками
        uri = None
        last_error = None
        
        for attempt in range(retries):
            try:
                uri = await upload_file_to_storage_async(file_path)
                print(f"Файл загружен в облако: {uri}")
                break
            except Exception as e:
                last_error = e
                print(f"Попытка {attempt + 1}/{retries} не удалась: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(retry_delay)
        
        if uri is None:
            print(f"Ошибка загрузки файла после {retries} попыток: {last_error}")
            return {"error": f"Ошибка загрузки файла после {retries} попыток: {last_error}"}
        
        # Добавляем URI к метаданным
        metadata['uri'] = uri
        
        # print(f"Файл {os.path.basename(file_path)} успешно обработан")
        return metadata
        
    except Exception as e:
        print(f"Ошибка обработки файла {file_path}: {e}")
        return {"error": f"Ошибка обработки файла: {e}"}


async def process_audio_files_async(records_dict: dict, max_concurrent_files: int = 10, retries: int = 3, retry_delay: float = 1.0) -> dict:
    """
    Асинхронно обрабатывает аудио файлы из словаря записей.
    Получает метаданные, загружает в облако и добавляет audio_metadata к каждой записи.
    
    :param records_dict: Словарь с записями звонков
    :param max_concurrent_files: Максимальное количество одновременных обработок файлов
    :param retries: Количество попыток при ошибках загрузки
    :param retry_delay: Задержка между попытками в секундах
    :return: Обновленный словарь с audio_metadata
    """
    print("Начинаю обработку аудио файлов")
    
    # Создаем копию исходного словаря
    result_dict = {}
    
    # Семафор для контроля параллельных операций
    semaphore = asyncio.Semaphore(max_concurrent_files)
    
    async def process_file_with_semaphore(file_path: str) -> dict:
        """Обрабатывает один файл с использованием семафора"""
        async with semaphore:
            metadata = await process_single_audio_file(file_path, retries, retry_delay)
            return metadata
    
    for portal_name, portal_data in records_dict.items():
        print(f"\nОбрабатываю портал: {portal_name}")
        
        # Копируем данные портала
        result_dict[portal_name] = portal_data.copy()
        
        records = portal_data.get('records', [])
        if not records:
            print(f"Нет записей для портала {portal_name}")
            continue
        
        # Собираем задачи для обработки файлов
        tasks = []
        record_indices = []
        
        for i, record in enumerate(records):
            record_id = record.get('id')
            if not record_id:
                print(f"Запись без ID, пропускаю: {record}")
                continue
            
            # Формируем путь к файлу
            file_path = f"bitrix24/downloads/{portal_name}/{record_id}.mp3"
            
            if os.path.exists(file_path):
                task = process_file_with_semaphore(file_path)
                tasks.append(task)
                record_indices.append(i)
            else:
                print(f"Файл не найден: {file_path}")
                # Добавляем ошибку в метаданные
                result_dict[portal_name]['records'][i]['audio_metadata'] = {
                    "error": f"Файл не найден: {file_path}"
                }
        
        if not tasks:
            print(f"Нет файлов для обработки в портале {portal_name}")
            continue
        
        print(f"Запускаю обработку {len(tasks)} файлов для портала {portal_name}")
        
        # Выполняем все задачи параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Добавляем результаты к записям
        successful_count = 0
        for i, (result, record_index) in enumerate(zip(results, record_indices)):
            if isinstance(result, Exception):
                print(f"Исключение при обработке файла: {result}")
                result_dict[portal_name]['records'][record_index]['audio_metadata'] = {
                    "error": f"Исключение: {result}"
                }
            else:
                result_dict[portal_name]['records'][record_index]['audio_metadata'] = result
                if "error" not in result:
                    successful_count += 1
        
        print(f"Портал {portal_name}: {successful_count}/{len(tasks)} файлов успешно обработано")
    
    print("Обработка аудио файлов завершена")
    return result_dict


if __name__ == "__main__":
    async def test():
        test_records = {
            "advertpro": {
                "records": [
                    {
                        "id": "493027",
                        "date": "2025-08-07T10:00:19+03:00",
                        "user_id": "11009",
                        "phone_number": "+79189616367"
                    },
                    {
                        "id": "493123",
                        "date": "2025-08-07T10:01:19+03:00",
                        "user_id": "15437",
                        "phone_number": "+79309620098"
                    }
                ],
                "entities": [],
                "users": []
            }
        }
        
        # Тестируем с пользовательскими параметрами
        processed_records = await process_audio_files_async(
            test_records, 
            max_concurrent_files=50, 
            retries=2, 
            retry_delay=0.5
        )
        
        from debug_utils import save_debug_json
        save_debug_json(processed_records, "audio_processed_test")
        
        print("Тест завершен. Параметры были:")
        print(f"- max_concurrent_files: 5")
        print(f"- retries: 2") 
        print(f"- retry_delay: 0.5 сек")
    
    asyncio.run(test())
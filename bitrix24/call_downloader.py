import asyncio
import aiohttp
import aiofiles
import os


async def download_call_by_id(portal_name, user_id, token, call_id):
    """
    Скачивает запись звонка по ID.
    
    :param portal_name: Название портала (например: advertpro)
    :param user_id: ID пользователя (например: 9)
    :param token: Токен доступа (например: eap2dc10t3z42q27)
    :param call_id: ID звонка для скачивания (например: 493123)
    :return: Путь к скачанному файлу или None при ошибке
    """
    print(f"Скачиваю звонок {call_id}")
    
    # Получаем данные звонка
    record_url = await get_call_record_url(portal_name, user_id, token, call_id)
    if not record_url:
        return None
    
    # Создаем папку для портала
    # Путь к папке downloads относительно текущего файла
    current_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(current_dir, "downloads", portal_name)
    os.makedirs(download_dir, exist_ok=True)
    
    # Формируем путь для сохранения
    file_path = os.path.join(download_dir, f"{call_id}.mp3")
    
    # Скачиваем файл
    if await download_audio_file(record_url, file_path):
        return file_path
    else:
        return None


async def get_call_record_url(portal_name, user_id, token, call_id):
    """
    Получает URL записи конкретного звонка через API Bitrix24.
    """
    url = f"https://{portal_name}.bitrix24.ru/rest/{user_id}/{token}/voximplant.statistic.get.json"
    params = {
        'FILTER[ID]': call_id
    }
    
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'result' in data and len(data['result']) > 0:
                        call_data = data['result'][0]
                        record_url = call_data.get('CALL_RECORD_URL')
                        
                        if record_url:
                            return record_url
                        else:
                            print(f"У звонка {call_id} нет записи")
                            return None
                    else:
                        print(f"Звонок {call_id} не найден")
                        return None
                else:
                    print(f"Ошибка API: {response.status}")
                    return None
                    
    except Exception as e:
        print(f"Ошибка при получении данных звонка {call_id}: {e}")
        return None


async def download_audio_file(record_url, file_path):
    """
    Скачивает аудиофайл по URL и сохраняет по указанному пути.
    """
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(record_url) as response:
                if response.status == 200:
                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    
                    file_size = os.path.getsize(file_path)
                    print(f"Файл сохранен: {file_path} ({file_size // 1024} KB)")
                    return True
                else:
                    print(f"Ошибка скачивания: {response.status}")
                    return False
                    
    except Exception as e:
        print(f"Ошибка при скачивании файла: {e}")
        return False


# Пример использования
if __name__ == "__main__":
    async def test():
        result = await download_call_by_id(
            portal_name="advertpro",
            user_id="9", 
            token="eap2dc10t3z42q27",
            call_id="493235"
        )
        print(f"Результат: {result}")
    
    asyncio.run(test())
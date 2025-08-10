import asyncio
import boto3
import os
from dotenv import load_dotenv
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor


async def upload_file_to_storage_async(file_path: str) -> str:
    """
    Асинхронно загружает файл в хранилище S3 и возвращает URI загруженного файла.
    Дополнительно выводит в консоль ответ S3 API.
    
    :param file_path: Путь к файлу для загрузки
    :return: URI загруженного файла
    """
    load_dotenv()

    access_key = os.getenv('API_KEY')
    secret_key = os.getenv('SECRET_KEY')
    # print(f"Используется ключ: {access_key}")

    if not access_key or not secret_key:
        raise Exception("Ошибка: Переменные окружения API_KEY и SECRET_KEY не установлены.")

    bucket_name = "inbound"
    endpoint_url = "https://s3.api.tinkoff.ai"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {file_path} не найден.")

    file_name = os.path.basename(file_path)

    def sync_upload():
        """Синхронная функция загрузки для выполнения в отдельном потоке"""
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                signature_version='s3v4',
                retries={'max_attempts': 3}
            )
        )

        with open(file_path, 'rb') as file_data:
            # Используем метод put_object, который возвращает ответ сервера
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=file_data
            )

        # # Печатаем "сырые" данные ответа S3
        # print("S3 API Response:", response)

        # Формируем URI
        uri = f"storage://{endpoint_url.replace('https://', '')}/{bucket_name}/{file_name}"
        return uri

    try:
        # Выполняем синхронную загрузку в отдельном потоке
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            uri = await loop.run_in_executor(executor, sync_upload)
        
        return uri

    except Exception as e:
        raise Exception(f"Ошибка при загрузке файла {file_path}: {str(e)}")


# Пример использования
if __name__ == "__main__":
    async def test():
        """Тест загрузки одного файла"""
        file_path = "bitrix24/downloads/advertpro/493027.mp3"
        
        if not os.path.exists(file_path):
            print(f"Файл для тестирования не найден: {file_path}")
            return
        
        try:
            print(f"Загружаю файл: {file_path}")
            uri = await upload_file_to_storage_async(file_path)
            print(f"✅ Файл успешно загружен. URI: {uri}")
        except Exception as e:
            print(f"Произошла ошибка: {e}")
    
    # Запускаем тест
    asyncio.run(test())
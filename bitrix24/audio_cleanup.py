import asyncio
import os
import shutil
import sys

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger_config import setup_logger

logger = setup_logger('audio_cleanup', 'logs/audio_cleanup.log')


def clean_audio_files_for_portal(portal_name: str):
    """Очищает папку с аудиофайлами для портала"""
    # Получаем абсолютный путь относительно расположения скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    downloads_path = os.path.join(script_dir, "downloads", portal_name)
    
    if not os.path.exists(downloads_path):
        logger.info(f"Папка {downloads_path} не существует")
        return
    
    try:
        for filename in os.listdir(downloads_path):
            file_path = os.path.join(downloads_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        logger.info(f"Папка {downloads_path} очищена")
    except Exception as e:
        logger.error(f"Ошибка очистки папки {downloads_path}: {e}")


async def cleanup_audio_files_after_db_upload(db_stats_dict: dict):
    """Очищает аудиофайлы для порталов с успешной загрузкой в БД"""
    logger.info("=== НАЧИНАЮ ОБРАБОТКУ: Очистка аудиофайлов ===")
    logger.info("Очищаю аудиофайлы после загрузки в БД")
    
    for portal_name, portal_data in db_stats_dict.items():
        success_rate = portal_data.get('db_update_stats', {}).get('success_rate', 0)
        
        if success_rate == 1.0:
            logger.info(f"Портал {portal_name}: успешно загружен - очищаю файлы")
            clean_audio_files_for_portal(portal_name)
        else:
            logger.warning(f"Портал {portal_name}: есть ошибки - файлы НЕ удаляю")
    
    logger.info("Очистка завершена")
    logger.info("=== ЗАВЕРШАЮ ОБРАБОТКУ: Очистка аудиофайлов ===")


if __name__ == "__main__":
    async def test():
        test_db_stats = {
            "advertpro": {
                "db_update_stats": {
                    "total_records": 2,
                    "updated_records": 2,
                    "success_rate": 1.0
                }
            }
        }
        
        await cleanup_audio_files_after_db_upload(test_db_stats)
    
    asyncio.run(test())

import json
import os
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):
    """Кастомный JSON энкодер для обработки datetime объектов"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def save_debug_json(data_dict, variable_name):
    """
    Сохраняет словарь в JSON файл для отладки.
    
    Args:
        data_dict (dict): Словарь для сохранения
        variable_name (str): Название переменной (будет именем файла)
    """
    # Создаем папку json_tests если её нет
    debug_folder = "json_tests"
    if not os.path.exists(debug_folder):
        os.makedirs(debug_folder)
    
    # Формируем путь к файлу
    file_path = os.path.join(debug_folder, f"{variable_name}.json")
    
    try:
        # Сохраняем с красивым форматированием и обработкой datetime
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(
                data_dict, 
                f, 
                indent=4, 
                ensure_ascii=False, 
                cls=DateTimeEncoder
            )
        
        print(f"✅ Отладочные данные сохранены: {file_path}")
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении отладочных данных: {e}")


def save_multiple_debug_json(**kwargs):
    """
    Сохраняет несколько переменных одновременно.
    
    Пример использования:
        save_multiple_debug_json(
            records=my_records,
            final_data=my_final_data,
            processed=my_processed
        )
    """
    for var_name, data in kwargs.items():
        save_debug_json(data, var_name)


def convert_datetime_to_string(obj):
    """
    Рекурсивно преобразует datetime объекты в строки для JSON сериализации.
    
    Args:
        obj: Объект для преобразования (может быть словарь, список, datetime или примитив)
    
    Returns:
        Объект с преобразованными datetime в строки
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_datetime_to_string(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_string(item) for item in obj]
    return obj


def print_json_safe(data, title="Debug Data"):
    """
    Безопасно выводит данные в JSON формате (обрабатывает datetime).
    
    Args:
        data: Данные для вывода
        title: Заголовок для вывода
    """
    try:
        print(f"\n=== {title} ===")
        print(json.dumps(data, indent=4, ensure_ascii=False, cls=DateTimeEncoder))
        print("=" * (len(title) + 8))
    except Exception as e:
        print(f"❌ Ошибка при выводе {title}: {e}")
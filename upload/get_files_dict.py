import os


def get_files_dict(folder_path: str) -> dict:
    """
    Возвращает словарь с именами файлов и их полными путями.

    Args:
        folder_path (str): Путь к папке.

    Returns:
        dict: Словарь, где ключ — имя файла, значение — полный путь.
    """
    if not os.path.isdir(folder_path):
        raise ValueError(f"Указанный путь не является папкой: {folder_path}")

    files_dict = {}

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):  # Проверяем, что это файл
            files_dict[file_name] = file_path

    return files_dict


# Пример использования
if __name__ == "__main__":
    folder = "audio"  # Укажите путь к папке
    try:
        result = get_files_dict(folder)
        print(result)  # Выводит словарь файлов
    except ValueError as e:
        print(e)
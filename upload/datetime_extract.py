from datetime import datetime

def extract_datetime_from_filename(file_name: str) -> datetime:
    """
    Извлекает дату и время из названия файла.

    Args:
        file_name (str): Название файла, например, "2025-01-21 10-34-00 +79166388987.mp3".

    Returns:
        datetime: Объект datetime, представляющий дату и время из названия файла.

    Raises:
        ValueError: Если название файла не соответствует ожидаемому формату.
    """
    try:
        # Убираем расширение файла
        base_name = file_name.split('.')[0]
        # Извлекаем дату и время (первые 19 символов)
        datetime_str = base_name[:19]
        # Преобразуем строку в объект datetime
        call_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H-%M-%S")
        return call_datetime
    except Exception as e:
        raise ValueError(f"Ошибка при извлечении даты и времени из названия файла '{file_name}': {e}")


# Пример использования
if __name__ == "__main__":
    file_name = "2025-01-21 10-34-00 +79166388987.mp3"
    try:
        extracted_datetime = extract_datetime_from_filename(file_name)
        print(f"Извлечённая дата и время: {extracted_datetime}")
    except ValueError as error:
        print(error)
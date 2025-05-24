from pydub import AudioSegment
import os


def split_stereo_mp3(mp3_path, output_dir="audio"):
    """
    Разделяет MP3-файл на два моно-файла (левый и правый канал).

    :param mp3_path: Путь к исходному MP3-файлу.
    :param output_dir: Папка для сохранения разделенных файлов.
    :return: Пути к полученным файлам.
    """
    # Загружаем MP3
    audio = AudioSegment.from_file(mp3_path, format="mp3")

    # Проверяем, стерео ли файл
    if audio.channels != 2:
        raise ValueError("Файл не является стерео!")

    # Разделяем на два канала
    left_channel = audio.split_to_mono()[0]
    right_channel = audio.split_to_mono()[1]

    # Создаем папку для выходных файлов
    os.makedirs(output_dir, exist_ok=True)

    # Генерируем пути к выходным файлам
    base_name = os.path.splitext(os.path.basename(mp3_path))[0]
    left_output = os.path.join(output_dir, f"{base_name}_left.mp3")
    right_output = os.path.join(output_dir, f"{base_name}_right.mp3")

    # Сохраняем каждый канал
    left_channel.export(left_output, format="mp3")
    right_channel.export(right_output, format="mp3")

    print(f"Файл разделен!\nЛевый канал: {left_output}\nПравый канал: {right_output}")
    return left_output, right_output


# Пример использования
mp3_file = "upload/audio/2024-12-11 11-37-49 +79153315033.mp3"  # Укажи путь к своему MP3
split_stereo_mp3(mp3_file)
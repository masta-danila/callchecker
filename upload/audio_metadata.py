import subprocess
import json

# Таблица преобразования codec_name в encoding
ENCODING_MAP = {
    "pcm_alaw": "ALAW",
    "pcm_mulaw": "MULAW",
    "pcm_s16le": "LINEAR16",
    "pcm_s16be": "LINEAR16",
    "opus": "RAW_OPUS",
    "mp3": "MPEG_AUDIO"
}


def get_audio_metadata(file_path):
    try:
        # Запуск ffprobe для извлечения метаданных
        command = [
            "ffprobe",
            "-v", "error",
            "-show_streams",
            "-select_streams", "a",
            "-show_entries", "stream=codec_name,sample_rate,channels,duration",
            "-of", "json",
            file_path
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            return {"error": result.stderr.strip()}

        # Парсинг вывода ffprobe
        metadata = json.loads(result.stdout)
        if "streams" not in metadata or not metadata["streams"]:
            return {"error": "No audio stream found"}

        audio_stream = metadata["streams"][0]
        codec_name = audio_stream.get("codec_name", "Unknown")

        # Преобразование codec_name в encoding
        encoding = ENCODING_MAP.get(codec_name, "Unknown")

        return {
            "encoding": encoding,
            "num_channels": audio_stream.get("channels", 0),
            "sample_rate_hertz": int(audio_stream.get("sample_rate", 0)),
            "duration": round(float(audio_stream.get("duration", 0.0)), 2)
        }
    except Exception as e:
        return {"error": str(e)}


# Пример использования
file_path = "audio/2025-01-29 16-32-34 +79067571133.mp3"  # Укажите путь к вашему файлу
audio_details = get_audio_metadata(file_path)
print(audio_details)
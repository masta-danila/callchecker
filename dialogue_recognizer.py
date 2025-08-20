import recognition_by_uri
import dialog_builder
import asyncio


async def process_record(record):
    audio_metadata = record.get("audio_metadata", {})
    uri = audio_metadata.get("uri")
    encoding = audio_metadata.get("encoding")
    num_channels = audio_metadata.get("num_channels")
    sample_rate_hertz = audio_metadata.get("sample_rate_hertz")

    # Проверяем, что все необходимые поля присутствуют
    if not uri or not encoding or num_channels is None or not sample_rate_hertz:
        print(f"Пропущена запись с неполными метаданными: {record}")
        return None

    print(f"Распознается файл: {uri}")

    try:
        raw_data = await recognition_by_uri.recognize_speech(
            uri,
            encoding,
            num_channels,
            sample_rate_hertz
        )
    except Exception as e:
        print(f"Ошибка при распознавании {uri}: {e}")
        return None
    # print("Сырые данные:", raw_data)

    if not raw_data:
        print(f"Не удалось получить сырые данные для URI: {uri}")
        return None

    dialogue = dialog_builder.build_dialog_from_response(raw_data)
    if not dialogue:
        print(f"Не удалось построить диалог для URI: {uri}, устанавливаю пустой диалог")
        dialogue = ""
        # Устанавливаем статус 'empty' для пустых диалогов
        record["status"] = "empty"
    else:
        # Устанавливаем статус 'recognized' для успешно распознанных диалогов
        record["status"] = "recognized"

    # Добавляем диалог в запись и возвращаем её (либо успешный, либо пустой)
    record["dialogue"] = dialogue
    if dialogue:
        print(f"Диалог для URI {uri} успешно сохранен со статусом 'recognized'.")
    else:
        print(f"Для URI {uri} установлен пустой диалог со статусом 'empty'.")
    return record


if __name__ == "__main__":
    sample_record = {
        "audio_metadata": {
        "uri": "storage://s3.api.tinkoff.ai/inbound/call_494197.mp3",
        "duration": 4.82,
        "encoding": "MPEG_AUDIO",
        "num_channels": 2,
        "sample_rate_hertz": 8000
        }
    }

    # Запускаем асинхронную функцию через asyncio.run(...)
    result = asyncio.run(process_record(sample_record))
    print(f"Результат: {result}")

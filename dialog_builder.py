import asyncio
from recognition_by_uri import recognize_speech


def build_dialog_from_response(response):
    """
    Создаёт диалог из сырого ответа сервиса, объединяя фразы одного канала,
    и возвращает строку, где каждая реплика записана с указанием канала.

    Параметры:
        response (stt_pb2.RecognizeResponse): Сырой ответ от сервиса распознавания.

    Возвращает:
        str: Строка, представляющая диалог по каналам.
    """
    dialog = []
    last_channel = None
    current_message = None

    # Сортируем результаты по времени начала фразы
    results = sorted(response.results, key=lambda r: r.start_time.ToTimedelta().total_seconds())

    for result in results:
        channel = result.channel  # Определяем канал (0 или 1)
        text = " ".join(alternative.transcript for alternative in result.alternatives).strip()

        # Пропускаем пустые сообщения
        if not text:
            continue

        # Если текущий результат относится к тому же каналу, что и предыдущий, объединяем текст
        if channel == last_channel:
            current_message['text'] += " " + text
        else:
            # Если канал сменился, сохраняем предыдущее сообщение и начинаем новое
            if current_message:
                dialog.append(current_message)
            current_message = {'channel': channel, 'text': text}
            last_channel = channel

    # Добавляем последнее сообщение
    if current_message:
        dialog.append(current_message)

    # Формируем единую строку вместо списка
    # Например, каждая реплика на новой строке в формате: "Канал <channel>: <text>"
    dialog_str_lines = []
    for message in dialog:
        dialog_str_lines.append(f"{message['channel']}: {message['text']}")

    # Склеиваем все строки в один многострочный текст
    return "\n".join(dialog_str_lines)


if __name__ == "__main__":
    async def main():
        """
        Асинхронная основная функция для распознавания речи и создания диалога.
        """
        uri = "storage://s3.api.tinkoff.ai/inbound/2025-02-19 13-38-04 +79255064127.mp3"
        encoding = "MPEG_AUDIO"  # Пример кодека
        num_channels = 2  # Количество каналов
        sample_rate_hertz = 8000  # Частота дискретизации

        # Асинхронно получаем сырой ответ от сервиса распознавания
        raw_response = await recognize_speech(uri, encoding, num_channels, sample_rate_hertz)

        # Создаём диалог из сырого ответа (уже строка)
        dialog_text = build_dialog_from_response(raw_response)

        print("Диалог:")
        print(dialog_text)


    asyncio.run(main())

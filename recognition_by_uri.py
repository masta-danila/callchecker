import asyncio
import grpc
from dotenv import load_dotenv
from tinkoff.cloud.stt.v1 import stt_pb2_grpc, stt_pb2
from tinkoff.cloud.longrunning.v1 import longrunning_pb2_grpc, longrunning_pb2
from tinkoff.cloud.longrunning.v1.longrunning_pb2 import OperationState, FAILED, DONE
from auth import generate_auth_metadata

# Загрузка переменных из .env файла
load_dotenv()

endpoint = "api.tinkoff.ai:443"


def build_recognize_request(uri, encoding, num_channels, sample_rate_hertz):
    """
    Создаёт запрос для распознавания речи с заданными параметрами.
    """
    request = stt_pb2.LongRunningRecognizeRequest()
    request.audio.uri = uri
    request.config.encoding = getattr(stt_pb2.AudioEncoding, encoding)
    request.config.sample_rate_hertz = sample_rate_hertz
    request.config.num_channels = num_channels
    return request


def build_get_operation_request(id):
    """
    Создаёт запрос для получения статуса операции.
    """
    request = longrunning_pb2.GetOperationRequest()
    request.id = id
    return request


async def recognize_speech(uri, encoding, num_channels, sample_rate_hertz):
    """
    Асинхронная функция, которая отправляет URI файла на распознавание
    и возвращает сырой объект RecognizeResponse.
    """
    try:
        # Создаём асинхронный канал и отправляем URI на распознавание
        async with grpc.aio.secure_channel(endpoint, grpc.ssl_channel_credentials()) as channel:
            stt_stub = stt_pb2_grpc.SpeechToTextStub(channel)

            # Получаем метаданные для авторизации
            stt_metadata = generate_auth_metadata("tinkoff.cloud.stt")
            operation = await stt_stub.LongRunningRecognize(
                build_recognize_request(uri, encoding, num_channels, sample_rate_hertz),
                metadata=stt_metadata
            )

            operations_stub = longrunning_pb2_grpc.OperationsStub(channel)
            operations_metadata = generate_auth_metadata("tinkoff.cloud.longrunning")

            # Ожидаем результаты, периодически проверяя состояние операции
            while operation.state != FAILED and operation.state != DONE:
                await asyncio.sleep(1)
                operation = await operations_stub.GetOperation(build_get_operation_request(operation.id), metadata=operations_metadata)

            if operation.state == FAILED:
                raise RuntimeError(f"Распознавание завершилось с ошибкой: {operation.error.message}")

            # Распаковываем и возвращаем сырой результат
            response = stt_pb2.RecognizeResponse()
            operation.response.Unpack(response)
            return response

    finally:
        print("Готово.")


if __name__ == "__main__":
    async def main():
        """
        Основная асинхронная функция для запуска распознавания.
        """
        # Входные данные
        uri = "storage://s3.api.tinkoff.ai/inbound/2025-01-29 16-32-34 +79067571133.mp3"
        encoding = "MPEG_AUDIO"  # Пример кодека
        num_channels = 2  # Количество каналов
        sample_rate_hertz = 8000  # Частота дискретизации

        raw_response = await recognize_speech(uri, encoding, num_channels, sample_rate_hertz)

        print("Сырой результат от сервиса:")
        print(raw_response)
    asyncio.run(main())

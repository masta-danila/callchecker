from dotenv import load_dotenv
import time
from openai import OpenAI

# 1. Инициализируем клиент
load_dotenv()
client = OpenAI()

# 2. Загрузка файла с данными для батча
with open("example_batch_input.jsonl", "rb") as f:
    uploaded_file = client.files.create(
        file=f,
        purpose="batch"
    )

print("Загруженный файл:", uploaded_file)
input_file_id = uploaded_file.id  # что-то вроде "file-abc123"

# 3. Создание батча
batch = client.batches.create(
    input_file_id=input_file_id,
    endpoint="/v1/chat/completions",
    completion_window="24h"
)
print("Созданный батч:", batch)
batch_id = batch.id  # "batch_abc123"

# 4. Ждём, пока батч не будет выполнен
while True:
    batch_info = client.batches.retrieve(batch_id)
    status = batch_info.status  # "validating", "in_progress", ...
    print("Статус батча:", status)

    if status in ["completed", "failed", "cancelled", "expired"]:
        break

    time.sleep(5)

if batch_info.status != "completed":
    print(f"Батч завершился в статусе {batch_info.status}.")
    # Если нужно посмотреть ошибки: batch_info.error_file_id
    exit(1)

print("Батч успешно завершён.")

# 5. Скачиваем выходной файл
output_file_id = batch_info.output_file_id
if not output_file_id:
    print("output_file_id пуст.")
    exit(1)

# Используем метод client.files.content(...) или client.files.download(...)
# В свежем клиенте это может выглядеть так:
file_resp = client.files.content(output_file_id)
result_text = file_resp.text
with open("batch_output.jsonl", "w", encoding="utf-8") as outfile:
    outfile.write(result_text)

print("Результат сохранён в batch_output.jsonl")

# 6. Парсим результат
#   Каждая строка — JSON, содержаший id, custom_id, response (c body) или error.
import json

with open("batch_output.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        # obj["custom_id"] — это тот же, что вы указывали в входном файле
        # obj["response"]["body"] — тело ответа от модели
        # obj["error"] — информация об ошибке, если запрос не выполнился.
        print(obj)
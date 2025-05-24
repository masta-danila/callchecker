import json
from openai import OpenAI
from dotenv import load_dotenv
from llm_response_cleaner import clean_llm_content


def request_gpt(model: str, messages: list) -> dict:
    """
    Синхронная функция, делающая запрос к OpenAI.
    Возвращает словарь с очищенным контентом и рассчитанной стоимостью.
    """
    load_dotenv()
    client = OpenAI()
    result = client.chat.completions.create(
        model=model,
        messages=messages
    )

    # Извлекаем сгенерированный ответ
    answer = result.choices[0].message.content

    # Очищаем ответ (удаляем возможные обёртки ```json и т. п.)
    answer = clean_llm_content(answer)

    # Загрузка тарифов из файла llm_price.json
    with open("llm_pricing.json", "r", encoding="utf-8") as f:
        pricing = json.load(f)

    # Извлекаем информацию о токенах из ответа API
    prompt_tokens = result.usage.prompt_tokens
    completion_tokens = result.usage.completion_tokens
    cached_tokens = result.usage.prompt_tokens_details.cached_tokens

    # Рассчитываем количество некэшированных prompt_tokens
    non_cached_prompt_tokens = prompt_tokens - cached_tokens

    # Получаем тарифы для выбранной модели
    model_pricing = pricing.get(model)
    if not model_pricing:
        raise Exception(f"Отсутствует информация о стоимости для модели: {model}")

    input_rate = model_pricing.get("1M input tokens")
    cached_rate = model_pricing.get("1M cached** input tokens")
    output_rate = model_pricing.get("1M output tokens")

    if input_rate is None or cached_rate is None or output_rate is None:
        raise Exception(f"Не указаны тарифы для входных, кэшированных или выходных "
                        f"токенов для модели: {model}")

    # Расчёт стоимости:
    # Стоимость некэшированных prompt_tokens
    cost_non_cached_prompt = (non_cached_prompt_tokens / 1_000_000) * input_rate
    # Стоимость кэшированных prompt_tokens
    cost_cached = (cached_tokens / 1_000_000) * cached_rate
    # Стоимость output токенов
    cost_output = (completion_tokens / 1_000_000) * output_rate
    total_cost = cost_non_cached_prompt + cost_cached + cost_output

    return {"content": answer, "cost": total_cost}


if __name__ == "__main__":
    completion = request_gpt(
        model='gpt-4o',
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What is 2 + 2?"},
            {"role": "assistant", "content": "2 + 2 equals 4."},
            {"role": "user", "content": "and + 2? Give just number"}
        ]
    )

    print(completion)
import asyncio
from functools import partial
from llm_router import llm_request
import ast

def calculate_evaluation(evaluation1, evaluation2):
    """
    Рассчитывает результирующую оценку по правилам:
    - Если обе None: None
    - Если одна None: возвращаем вторую
    - Если обе не None: среднее (float)
    """
    if evaluation1 is None and evaluation2 is None:
        return None
    if evaluation1 is None:
        return evaluation2
    if evaluation2 is None:
        return evaluation1
    return round((float(evaluation1) + float(evaluation2)) / 2, 2)

async def sum_text_blocks(text1, evaluation1, text2, evaluation2, max_size):
    """
    Суммирует два блока текста/списка/других структур данных через LLM или возвращает text2, если text1 пустой.

    :param text1: Первый блок текста, списка или другой структуры данных для суммирования.
    :param evaluation1: Оценка первого блока (число или None).
    :param text2: Второй блок текста, списка или другой структуры данных для суммирования.
    :param evaluation2: Оценка второго блока (число или None).
    :param max_size: Максимально допустимый размер итогового блока в количестве слов.
    :return: Словарь с ключами:
        - "text_result": строка с суммированным (объединённым) блоком данных;
        - "evaluation_result": итоговая оценка (среднее двух, одна из оценок, или None по правилам).
    """
    evaluation_result = calculate_evaluation(evaluation1, evaluation2)

    if text1 == '':
        return {
            "text_result": text2,
            "evaluation_result": evaluation_result
        }

    prompt = (
        "Выполни суммирование двух блоков текста/списков/других структур данных. ВАЖНО: сохрани структуру и тип составляющих блоков в итоговом ответе. "
        "Повторяющиеся или пересекающиеся пункты должны быть объединены."
        "Если в исходных данных были тексты, то результатом должен быть общий текст, если списки — то результатом должен быть общий список и т.д. "
        f"Итоговый блок не должен превышать {max_size} слов. Если суммированный блок превышает {max_size} слов, постарайтесь уплотнить его, сохраняя все основные идеи и смысл обоих блоков, не теряя важной информации.\n"
        "Блок 1:\n"
        f"{text1}\n"
        "Блок 2:\n"
        f"{text2}\n"
        "Ответ выдай строго в виде словаря следующего синтаксиса (без доп символов и кавычек, синтаксис словаря должен быть с\n"
        "таким же набором фигурных скобок и двойных кавычек, обрамлять словарь в доп символы запрещено):\n"
        "{\"text\": \"суммирующий блок\",\n"
        "Кроме словаря вставлять что-то в ответ запрещено.\n"
        "Кавычки внутри ответа на промпт экранируй.\n"
    )

    messages = [
        {"role": "user", "content": prompt}
    ]
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(
        None,
        partial(llm_request,
                model="gpt-4o-mini",
                messages=messages)
    )
    try:
        raw = response["content"].strip()
        result_dict = ast.literal_eval(raw)
        text_result = result_dict['text']
    except Exception as e:
        print(f"Ошибка при парсинге ответа: {e}\nОтвет модели: {response}")
        text_result = ''

    return {
        "text_result": text_result,
        "evaluation_result": evaluation_result
    }
# Пример использования
if __name__ == "__main__":
    async def main():
        text1 = """
            - осуществляют разработку сайтов;
            - планируют использовать SEO и контекстную рекламу для продвижения;
            - интересуются уникальными текстами для разделов сайта;
            - имеют 13 сайтов;
            - на одном из сайтов установили авито.ру как конкурента;
            - планируют открыть ИП в мае;
            - предпочитают оформить сайт вначале на физическое лицо;
            - рассматривают возможность патентования сайта;
            - обсуждают использование старых доменов для передачи веса;
            - требуют уникальное название для нового сайта; 
        """
        evaluation1 = 5
        text2 = """
            - у компании есть 13 сайтов;
            - активны в сфере дверной продукции;
            - настраивают индексацию сайта;
            - планируют встречу с SEO специалистом;
            - сталкиваются с проблемой креативности в названиях доменов.
        """
        evaluation2 = 4
        max_size = 500

        result = await sum_text_blocks(text1, evaluation1, text2, evaluation2, max_size)
        print(result)

    asyncio.run(main())
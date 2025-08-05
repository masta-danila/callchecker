import asyncio
from functools import partial
from llm_router import llm_request
import ast

def calculate_evaluation(*evaluations):
    """
    Рассчитывает результирующую оценку для любого количества значений по правилам:
    - Если все None: None
    - Если есть не None значения: среднее арифметическое (float)
    
    :param evaluations: Любое количество оценок (могут быть None)
    :return: Среднее значение или None
    """
    # Фильтруем только не None значения
    valid_evaluations = [eval for eval in evaluations if eval is not None]
    
    if not valid_evaluations:
        return None
    
    # Вычисляем среднее арифметическое
    average = sum(float(eval) for eval in valid_evaluations) / len(valid_evaluations)
    return round(average, 2)

async def sum_text_blocks(text_evaluation_pairs, max_size):
    """
    Суммирует любое количество блоков текста через LLM.

    :param text_evaluation_pairs: Список кортежей (text, evaluation) для суммирования.
                                  Например: [("текст1", 4.5), ("текст2", None), ("текст3", 3.8)]
    :param max_size: Максимально допустимый размер итогового блока в количестве слов.
    :return: Словарь с ключами:
        - "text_result": строка с суммированным блоком данных;
        - "evaluation_result": итоговая средняя оценка всех блоков.
    """
    if not text_evaluation_pairs:
        return {"text_result": "", "evaluation_result": None}
    
    # Извлекаем тексты и оценки
    texts = [pair[0] for pair in text_evaluation_pairs]
    evaluations = [pair[1] for pair in text_evaluation_pairs]
    
    # Фильтруем пустые тексты
    non_empty_texts = [text for text in texts if text and text.strip()]
    
    if not non_empty_texts:
        return {"text_result": "", "evaluation_result": calculate_evaluation(*evaluations)}
    
    if len(non_empty_texts) == 1:
        return {
            "text_result": non_empty_texts[0],
            "evaluation_result": calculate_evaluation(*evaluations)
        }
    
    # Создаем промпт для суммирования множественных блоков
    prompt = (
        f"Выполни суммирование {len(non_empty_texts)} блоков текста/списков/других структур данных."
        "ВАЖНО: сохрани структуру и тип составляющих блоков в итоговом ответе."
        "То есть если в исходных данных были тексты, то результатом должен быть общий текст, если списки — то результатом должен быть общий список и т.д."
        "Повторяющиеся или пересекающиеся пункты должны быть объединены."
        f"Итоговый блок не должен превышать {max_size} слов. Если суммированный блок превышает {max_size} слов, постарайтесь уплотнить его, сохраняя все основные идеи и смысл всех блоков, не теряя важной информации.\n"
    )
    
    # Добавляем все блоки в промпт
    for i, text in enumerate(non_empty_texts, 1):
        prompt += f"Блок {i}:\n{text}"
    
    prompt += (
        "Ответ выдай строго в виде словаря следующего синтаксиса (без доп символов и кавычек, синтаксис словаря должен быть с "
        "таким же набором фигурных скобок и двойных кавычек, обрамлять словарь в доп символы запрещено):\n"
        "{\"text\": \"суммирующий блок\"}\n"
        "Кроме словаря вставлять что-то в ответ запрещено.\n"
        "Кавычки внутри ответа на промпт экранируй.\n"
    )

    messages = [{"role": "user", "content": prompt}]
    
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(
        None,
        partial(llm_request, model="gpt-4o-mini", messages=messages)
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
        "evaluation_result": calculate_evaluation(*evaluations)
    }



# Пример использования
if __name__ == "__main__":
    async def main():
        # Пример суммирования множественных блоков
        text_eval_pairs = [
            ("- Клиент ранее взаимодействовал с юридическими вопросами, связанными с патентами.\n- Есть представление о стоимости патентования.\n- Оформление сайта на физическое лицо.\n", 5),
            ("- Разработка сайта начинается с сайта, который уже существует.\n- Имеется один основной сайт, привязанный к другому. \n", 5),
            ("- Уже осуществляется регистрация доменов.\n- Наличие у клиента нескольких сайтов.\n- Потребность в уникальном названии для новых доменов.\n- Наличие требований к соглашению при регистрации домена.\n- Интерес к продвижению и контекстной рекламе.", 3.5)
        ]
        
        result = await sum_text_blocks(text_eval_pairs, 500)
        print("Результат суммирования 5 блоков:", result)

    asyncio.run(main())
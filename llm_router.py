from gpt_request import request_gpt
from deepseek_request import request_deepseek
from claude_request import request_claude


def llm_request(model: str, messages: list) -> dict:
    """
    Принимает название модели и список сообщений.
    Вызывает соответствующую функцию запроса в зависимости от названия модели.
    """
    if model.startswith("gpt-"):
        return request_gpt(model, messages)
    elif model.startswith("deepseek-"):
        return request_deepseek(model, messages)
    elif model.startswith("claude-"):
        return request_claude(model, messages)
    else:
        raise Exception(f"Модель {model} не поддерживается.")


if __name__ == "__main__":
    import json

    # Выполняем запрос к выбранной модели
    response = llm_request(model="deepseek-reasoner",
                           messages=[
                               {"role": "user", "content": "Сколько будет два плюс два?"}
                           ]
                           )

    # Выводим ответ в формате JSON
    print(json.dumps(response, ensure_ascii=False, indent=2))

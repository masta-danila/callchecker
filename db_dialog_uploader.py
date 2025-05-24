import psycopg2.extras
from db_client import get_db_client


def upload_recognized_dialogs(dialogues_dict, status):
    """
    Обновляет в таблицах только поле 'dialogue' (из входного словаря)
    и поле 'status', значение которого передаётся в параметре функции,
    по записям с указанным 'id'.

    Новый формат входных данных:
    {
        'table_name': {
            'records': [
                {'id': '...', 'dialogue': '...', 'audio_metadata': {...}},
                ...
            ]
        },
        ...
    }
    """
    connection = get_db_client()
    try:
        with connection.cursor() as cursor:
            for table_name, table_data in dialogues_dict.items():
                records = table_data.get("records", [])
                if not records:
                    continue

                data_for_update = []
                for rec in records:
                    record_id = rec.get("id")
                    dialogue_text = rec.get("dialogue", "")
                    if record_id:
                        data_for_update.append((record_id, dialogue_text, status))

                if not data_for_update:
                    continue

                update_query = f"""
                    UPDATE {table_name} AS t
                    SET dialogue = v.dialogue,
                        status   = v.status::status_enum
                    FROM (VALUES %s) AS v(id, dialogue, status)
                    WHERE t.id = v.id
                """

                psycopg2.extras.execute_values(
                    cursor,
                    update_query,
                    data_for_update,
                    template="(%s, %s, %s)"
                )

        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Ошибка при загрузке диалогов:", e)
        raise
    finally:
        connection.close()
        print("Загрузка диалогов завершена.")


if __name__ == "__main__":
    # Пример входных данных в новом формате
    test_dialogues = {
        'advertpro': {
            'records': [
                {
                    'id': '2025-01-29 16-32-34 +79067571133.mp3',
                    'dialogue': '1: алло да\n0: алло\n1: сбросилось в общем тогда завтра я вам все высылаю списываемся убираем время и обсуждаем вот уже т бизнес нет все евгений хорошего вечера до завтра\n0: да угу да да да да спасибо до завтра\n1: до свидания',
                    'audio_metadata': {
                        'uri': 'storage://s3.api.tinkoff.ai/inbound/2025-01-29 16-32-34 +79067571133.mp3',
                        'duration': 14.83,
                        'encoding': 'MPEG_AUDIO',
                        'num_channels': 2,
                        'sample_rate_hertz': 8000
                    }
                },
                {
                    'id': '2025-03-11 15-11-25 +79214078077.mp3',
                    'dialogue': '1: добрый день\n0: алло здравствуйте\n1: здравствуйте\n0: я позвонила в объезд про\n1: да все верно\n0: попала да хотела бы обсудить вот продвижение сайта с кем можно пообщаться\n1: со мной меня роман зовут как вас\n0: меня зовут анна\n1: а\n0: роман смотрите\n1: да угу\n0: да ну сразу к делу\n1: конечно\n0: да ищем подрядчика на следующий сезон ...',
                    'audio_metadata': {
                        'uri': 'storage://s3.api.tinkoff.ai/inbound/2025-03-11 15-11-25 +79214078077.mp3',
                        'duration': 811.66,
                        'encoding': 'MPEG_AUDIO',
                        'num_channels': 2,
                        'sample_rate_hertz': 8000
                    }
                }
            ]
        },
        'test': {
            'records': [
                {
                    'id': '2025-01-29 16-32-34 +79067571133.mp3',
                    'dialogue': '1: алло да\n0: алло\n1: сбросилось в общем тогда завтра я вам все высылаю списываемся убираем время и обсуждаем вот уже т бизнес нет все евгений хорошего вечера до завтра\n0: да угу да да да да спасибо до завтра\n1: до свидания',
                    'audio_metadata': {
                        'uri': 'storage://s3.api.tinkoff.ai/inbound/2025-01-29 16-32-34 +79067571133.mp3',
                        'duration': 14.83,
                        'encoding': 'MPEG_AUDIO',
                        'num_channels': 2,
                        'sample_rate_hertz': 8000
                    }
                },
                {
                    'id': '2025-03-11 15-11-25 +79214078077.mp3',
                    'dialogue': '1: добрый день\n0: алло здравствуйте\n1: здравствуйте\n0: я позвонила в объезд про\n1: да все верно\n0: попала да хотела бы обсудить вот продвижение сайта с кем можно пообщаться\n1: со мной меня роман зовут как вас\n0: меня зовут анна\n1: а\n0: роман смотрите\n1: да угу\n0: да ну сразу к делу\n1: конечно\n0: да ищем подрядчика на следующий сезон ...',
                    'audio_metadata': {
                        'uri': 'storage://s3.api.tinkoff.ai/inbound/2025-03-11 15-11-25 +79214078077.mp3',
                        'duration': 811.66,
                        'encoding': 'MPEG_AUDIO',
                        'num_channels': 2,
                        'sample_rate_hertz': 8000
                    }
                }
            ]
        }
    }

    # Пример вызова функции с указанием статуса 'fixed'
    upload_recognized_dialogs(test_dialogues, 'fixed')
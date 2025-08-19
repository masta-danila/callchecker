import psycopg2.extras
from db_client import get_db_client


def upload_recognized_dialogs(dialogues_dict, default_status=None):
    """
    Обновляет в таблицах поля 'dialogue', 'summary' (из входного словаря)
    и поле 'status'. Статус берется из каждой записи (если есть поле 'status'),
    или используется default_status, если задан.

    Новый формат входных данных:
    {
        'table_name': {
            'records': [
                {'id': '...', 'dialogue': '...', 'summary': '...', 'status': 'empty'/'recognized'/'fixed', 'audio_metadata': {...}},
                ...
            ]
        },
        ...
    }
    """
    with get_db_client() as connection:
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
                        summary_text = rec.get("summary", "")
                        # Берем статус из записи, или используем default_status
                        record_status = rec.get("status", default_status)
                        if record_id and record_status:
                            data_for_update.append((record_id, dialogue_text, summary_text, record_status))

                    if not data_for_update:
                        continue

                    update_query = f"""
                        UPDATE {table_name} AS t
                        SET dialogue = v.dialogue,
                            summary  = v.summary,
                            status   = v.status::status_enum
                        FROM (VALUES %s) AS v(id, dialogue, summary, status)
                        WHERE t.id = v.id
                    """

                    psycopg2.extras.execute_values(
                        cursor,
                        update_query,
                        data_for_update,
                        template="(%s, %s, %s, %s)"
                    )

            connection.commit()
            print("Загрузка диалогов завершена.")
        except Exception as e:
            connection.rollback()
            print("Ошибка при загрузке диалогов:", e)
            raise


if __name__ == "__main__":
    # Пример входных данных в новом формате
    test_dialogues = {
        'advertpro': {
            'records': [
                {
                    'id': '2025-01-29 16-32-34 +79067571133.mp3',
                    'dialogue': 'М: Алло, да.\nК: Алло.\nМ: Сбросилось, в общем, тогда завтра я вам все высылаю, списываемся, убираем время и обсуждаем. Вот уже все, Евгений, хорошего вечера, до завтра.\nК: Да, угу, да, да, да, да, спасибо, до завтра.\nМ: До свидания.',
                    'summary': 'Менеджер Евгений договорился с клиентом о встрече на завтра для обсуждения предложения.',
                    'status': 'fixed',
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
                    'dialogue': 'М: Добрый день.\nК: Алло, здравствуйте.\nМ: Здравствуйте.\nК: Я позвонила в АдвертПро.\nМ: Да, все верно.\nК: Попала, да, хотела бы обсудить продвижение сайта, с кем можно пообщаться?\nМ: Со мной, меня Роман зовут, как вас?\nК: Меня зовут Анна...',
                    'summary': 'Клиент Анна обратилась к менеджеру Роману в компанию АдвертПро для обсуждения услуг по продвижению сайта компании-производителя гидроизоляции.',
                    'status': 'fixed',
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
        # 'test': {
        #     'records': [
        #         {
        #             'id': '2025-01-29 16-32-34 +79067571133.mp3',
        #             'dialogue': '1: алло да\n0: алло\n1: сбросилось в общем тогда завтра я вам все высылаю списываемся убираем время и обсуждаем вот уже т бизнес нет все евгений хорошего вечера до завтра\n0: да угу да да да да спасибо до завтра\n1: до свидания',
        #             'audio_metadata': {
        #                 'uri': 'storage://s3.api.tinkoff.ai/inbound/2025-01-29 16-32-34 +79067571133.mp3',
        #                 'duration': 14.83,
        #                 'encoding': 'MPEG_AUDIO',
        #                 'num_channels': 2,
        #                 'sample_rate_hertz': 8000
        #             }
        #         },
        #         {
        #             'id': '2025-03-11 15-11-25 +79214078077.mp3',
        #             'dialogue': '1: добрый день\n0: алло здравствуйте\n1: здравствуйте\n0: я позвонила в объезд про\n1: да все верно\n0: попала да хотела бы обсудить вот продвижение сайта с кем можно пообщаться\n1: со мной меня роман зовут как вас\n0: меня зовут анна\n1: а\n0: роман смотрите\n1: да угу\n0: да ну сразу к делу\n1: конечно\n0: да ищем подрядчика на следующий сезон ...',
        #             'audio_metadata': {
        #                 'uri': 'storage://s3.api.tinkoff.ai/inbound/2025-03-11 15-11-25 +79214078077.mp3',
        #                 'duration': 811.66,
        #                 'encoding': 'MPEG_AUDIO',
        #                 'num_channels': 2,
        #                 'sample_rate_hertz': 8000
        #             }
        #         }
        #     ]
        # }
    }

    # Пример вызова функции с default_status как fallback
    upload_recognized_dialogs(test_dialogues, default_status='fixed')
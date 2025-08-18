"""
Google Sheets Uploader - загрузка данных в Google таблицы

Этот модуль загружает данные из БД в Google таблицы согласно настройкам в bitrix24/bitrix_portals.json

Основные функции:
- upload_records_to_sheets: Загрузка записей в лист "Звонки"
- upload_entities_to_sheets: Загрузка сущностей в лист "Сущности"

Автор: AI Assistant
Дата: 2024
"""

import asyncio
import json
import os
import sys
from typing import Dict, List, Any

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from google.oauth2.service_account import Credentials
from debug_utils import save_debug_json


def load_google_credentials():
    """Загружает учетные данные Google Sheets"""
    credentials_path = os.path.join(os.path.dirname(__file__), '..', 'bitrix24', 'google_sheets_credentials.json')
    
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Файл учетных данных не найден: {credentials_path}")
    
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scope)
    return gspread.authorize(credentials)


def load_portal_settings():
    """Загружает настройки порталов из bitrix_portals.json"""
    portals_path = os.path.join(os.path.dirname(__file__), '..', 'bitrix24', 'bitrix_portals.json')
    
    if not os.path.exists(portals_path):
        raise FileNotFoundError(f"Файл настроек порталов не найден: {portals_path}")
    
    with open(portals_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Преобразуем массив порталов в словарь по названию портала
    portals_dict = {}
    for portal in config.get('portals', []):
        # Извлекаем название портала из URL
        url = portal.get('url', '')
        if 'bitrix24.ru' in url:
            portal_name = url.split('//')[1].split('.')[0]  # Извлекаем название из URL
            portals_dict[portal_name] = portal
    
    return portals_dict


def calculate_evaluation_from_record_data(record_criteria_list: List[Dict], criteria_config: List[Dict]) -> float:
    """
    Вычисляет среднее арифметическое оценок из data['criteria'] записи
    
    :param record_criteria_list: Список критериев из data['criteria'] записи
    :param criteria_config: Конфигурация критериев из БД
    :return: Среднее арифметическое оценок
    """
    if not record_criteria_list or not criteria_config:
        return 0.0
    
    # Создаем словарь конфигурации критериев по ID
    criteria_config_dict = {c['id']: c for c in criteria_config}
    
    scores = []
    for record_criterion in record_criteria_list:
        criterion_id = record_criterion.get('id')
        evaluation = record_criterion.get('evaluation')
        
        # Проверяем, есть ли конфигурация для этого критерия
        criterion_config = criteria_config_dict.get(criterion_id)
        if not criterion_config:
            continue
            
        # Учитываем только критерии с include_in_score=True
        if not criterion_config.get('include_in_score', False):
            continue
            
        # Добавляем оценку если она есть и это число
        if evaluation is not None and isinstance(evaluation, (int, float)):
            scores.append(float(evaluation))
    
    # Округляем до десятых (1 знак после запятой)
    average = sum(scores) / len(scores) if scores else 0.0
    return round(average, 1)


def calculate_evaluation(data: Dict, criteria: List[Dict], include_in_score_only: bool = True, include_in_entity_description: bool = False) -> float:
    """
    Вычисляет среднее арифметическое оценок по критериям
    
    :param data: Данные с оценками критериев
    :param criteria: Список критериев с настройками
    :param include_in_score_only: Учитывать только критерии с include_in_score=True
    :param include_in_entity_description: Дополнительно учитывать include_in_entity_description=True
    :return: Среднее арифметическое оценок
    """
    if not data or not criteria:
        return 0.0
    
    scores = []
    criteria_dict = {str(c['id']): c for c in criteria}
    
    for criterion_id, criterion_data in data.items():
        if not isinstance(criterion_data, dict):
            continue
            
        criterion_info = criteria_dict.get(str(criterion_id))
        if not criterion_info:
            continue
            
        # Проверяем условия включения в расчет
        if include_in_score_only and not criterion_info.get('include_in_score', False):
            continue
            
        if include_in_entity_description and not criterion_info.get('include_in_entity_description', False):
            continue
            
        score = criterion_data.get('score')
        if score is not None and isinstance(score, (int, float)):
            scores.append(float(score))
    
    # Округляем до десятых (1 знак после запятой)
    average = sum(scores) / len(scores) if scores else 0.0
    return round(average, 1)


def analyze_existing_worksheet(worksheet) -> Dict:
    """
    Анализирует существующий лист и возвращает информацию о нем
    
    :param worksheet: Объект Worksheet
    :return: Словарь с информацией о листе
    """
    if worksheet.row_count == 0:
        return {
            'existing_headers': [],
            'existing_records': [],
            'existing_record_ids': set(),
            'total_rows': 0
        }
    
    # Получаем заголовки (первая строка)
    existing_headers = worksheet.row_values(1) if worksheet.row_count > 0 else []
    
    # Получаем все существующие данные (кроме заголовков)
    existing_records = []
    existing_record_ids = set()
    
    if worksheet.row_count > 1:
        all_values = worksheet.get_all_values()
        
        # Пропускаем заголовки (первая строка)
        for row in all_values[1:]:
            if row and len(row) > 0:  # Проверяем что строка не пустая
                existing_records.append(row)
                # ID обычно в первой колонке
                if row[0]:  # Если есть ID
                    existing_record_ids.add(row[0])
    
    return {
        'existing_headers': existing_headers,
        'existing_records': existing_records,
        'existing_record_ids': existing_record_ids,
        'total_rows': len(existing_records)
    }


def normalize_headers_for_comparison(headers: List[str], criteria: List[Dict]) -> List[str]:
    """
    Нормализует заголовки для корректного сравнения.
    Преобразует _text/_eval колонки объединенных критериев в единые имена.
    
    :param headers: Исходные заголовки
    :param criteria: Список критериев для анализа
    :return: Нормализованные заголовки
    """
    normalized = []
    base_headers = ['id', 'date', 'phone_number', 'manager', 'category', 'evaluation', 'dialogue']
    
    # Создаем карту объединенных критериев
    merged_criteria = {}
    for criterion in criteria:
        show_text = criterion.get('show_text_description', False)
        show_evaluation = criterion.get('evaluate_criterion', False)
        if show_text and show_evaluation:
            criterion_name = criterion['name']
            merged_criteria[f"{criterion_name}_text"] = criterion_name
            merged_criteria[f"{criterion_name}_eval"] = criterion_name
    
    i = 0
    while i < len(headers):
        header = headers[i].strip()
        
        if header in base_headers:
            # Базовый заголовок - добавляем как есть
            normalized.append(header)
            i += 1
        elif header in merged_criteria:
            # Это _text или _eval от объединенного критерия
            criterion_name = merged_criteria[header]
            
            # Проверяем, есть ли парная колонка
            text_header = f"{criterion_name}_text"
            eval_header = f"{criterion_name}_eval"
            
            if (i + 1 < len(headers) and 
                headers[i + 1].strip() in merged_criteria and 
                merged_criteria[headers[i + 1].strip()] == criterion_name):
                # Есть парная колонка - объединяем в одно название
                if header not in normalized:  # Избегаем дубликатов
                    normalized.append(criterion_name)
                i += 2  # Пропускаем обе колонки
            else:
                # Нет парной колонки - добавляем как есть
                normalized.append(header)
                i += 1
        else:
            # Обычный заголовок критерия
            normalized.append(header)
            i += 1
    
    return normalized


def add_missing_columns(worksheet, existing_headers: List[str], new_headers: List[str], criteria: List[Dict]):
    """
    Добавляет недостающие колонки в конец листа
    
    :param worksheet: Объект Worksheet
    :param existing_headers: Существующие заголовки
    :param new_headers: Новые заголовки (полный список)
    :param criteria: Список критериев для нормализации
    :return: (updated_headers, structure_changed)
    """
    # Очищаем заголовки от пустых строк для корректного сравнения
    clean_existing = [h.strip() for h in existing_headers if h.strip()]
    clean_new = [h.strip() for h in new_headers if h.strip()]
    
    # Нормализуем заголовки для сравнения (объединяем _text/_eval в единые имена)
    normalized_existing = normalize_headers_for_comparison(clean_existing, criteria)
    normalized_new = normalize_headers_for_comparison(clean_new, criteria)
    
    print(f"📊 СРАВНЕНИЕ ЗАГОЛОВКОВ:")
    print(f"  Существующие (исходные): {len(clean_existing)}")
    print(f"  Существующие (нормализованные): {len(normalized_existing)}")
    print(f"  Новые (исходные): {len(clean_new)}")
    print(f"  Новые (нормализованные): {len(normalized_new)}")
    
    # Сравниваем нормализованные версии
    if set(normalized_existing) == set(normalized_new):
        print("✅ Структура заголовков не изменилась (после нормализации)")
        return clean_existing, False  # Структура НЕ изменилась
    
    # Находим недостающие заголовки в нормализованном виде
    missing_normalized = []
    for norm_header in normalized_new:
        if norm_header not in normalized_existing:
            missing_normalized.append(norm_header)
    
    if missing_normalized:
        print(f"⚠️ Обнаружены различия в структуре:")
        print(f"  Недостающие (нормализованные): {missing_normalized}")
        
        # Находим реальные недостающие заголовки в исходном формате
        missing_headers = []
        for header in clean_new:
            if header not in clean_existing:
                missing_headers.append(header)
        
        if missing_headers:
            print(f"📥 Добавляю {len(missing_headers)} новых колонок: {missing_headers[:5]}{'...' if len(missing_headers) > 5 else ''}")
            
            # Обновляем заголовки
            updated_headers = clean_existing + missing_headers
            
            # Расширяем лист если нужно больше колонок
            if len(updated_headers) > worksheet.col_count:
                worksheet.add_cols(len(updated_headers) - worksheet.col_count)
            
            # Обновляем первую строку с заголовками
            worksheet.update(f'1:{len(updated_headers)}', [updated_headers])
            
            return updated_headers, True  # Структура изменилась
        else:
            print("🔄 Структурные изменения без новых колонок - обновляю порядок")
            
            # Расширяем лист если нужно больше колонок
            if len(clean_new) > worksheet.col_count:
                worksheet.add_cols(len(clean_new) - worksheet.col_count)
            
            # Обновляем заголовки в правильном порядке
            worksheet.update(f'1:{len(clean_new)}', [clean_new])
            
            return clean_new, True  # Структура изменилась (порядок)
    else:
        print("✅ Колонки полностью совпадают")
        return clean_existing, False  # Структура НЕ изменилась


def filter_new_records(new_rows: List[List[str]], existing_record_ids: set, headers: List[str]) -> List[List[str]]:
    """
    Фильтрует новые записи, исключая уже существующие в листе
    
    :param new_rows: Все новые строки
    :param existing_record_ids: Множество существующих ID записей
    :param headers: Заголовки для определения позиции ID
    :return: Список только новых записей
    """
    if not new_rows or not headers:
        return new_rows
    
    # ID обычно в первой колонке
    id_index = 0
    if 'id' in headers:
        id_index = headers.index('id')
    
    filtered_rows = []
    
    for row in new_rows:
        if len(row) > id_index and row[id_index]:
            record_id = row[id_index]
            if record_id not in existing_record_ids:
                filtered_rows.append(row)
        else:
            # Если нет ID, добавляем запись (может быть пустая строка)
            filtered_rows.append(row)
    
    original_count = len(new_rows)
    filtered_count = len(filtered_rows)
    duplicate_count = original_count - filtered_count
    
    print(f"Фильтрация записей: {original_count} всего, {filtered_count} новых, {duplicate_count} дубликатов")
    
    return filtered_rows


def insert_new_records_at_bottom(worksheet, new_rows: List[List[str]], existing_headers: List[str], final_headers: List[str]):
    """
    Добавляет новые записи в конец листа, сохраняя старые данные
    
    :param worksheet: Объект Worksheet
    :param new_rows: Новые строки для добавления
    :param existing_headers: Исходные заголовки листа
    :param final_headers: Финальные заголовки (с новыми колонками)
    """
    if not new_rows:
        print("Нет новых записей для добавления")
        return
    
    # Адаптируем данные к новому формату заголовков (если добавились новые колонки)
    adapted_rows = []
    
    for row in new_rows:
        if len(existing_headers) == len(final_headers):
            # Заголовки не изменились, используем строку как есть
            adapted_rows.append(row)
        else:
            # Заголовки изменились, нужно адаптировать строку
            adapted_row = [''] * len(final_headers)
            
            # Заполняем значения согласно позициям в исходных заголовках
            for i, value in enumerate(row):
                if i < len(existing_headers):
                    header = existing_headers[i]
                    if header in final_headers:
                        new_index = final_headers.index(header)
                        adapted_row[new_index] = value
            
            adapted_rows.append(adapted_row)
    
    print(f"Добавляю {len(adapted_rows)} новых записей в конец листа")
    
    # Добавляем строки в конец листа
    if adapted_rows:
        worksheet.append_rows(adapted_rows)
        print(f"✅ Добавлено {len(adapted_rows)} записей")


def apply_all_formatting_batch(worksheet, headers: List[str], criterion_headers_info: List[Dict], total_rows: int, need_formatting: bool = True):
    """
    Применяет все форматирование одним batch-запросом: ширина колонок, объединение ячеек, стили
    
    :param worksheet: Объект Worksheet
    :param headers: Список заголовков
    :param criterion_headers_info: Информация о критериях для объединения
    :param total_rows: Общее количество строк для форматирования
    :param need_formatting: Нужно ли применять форматирование (только если структура изменилась)
    """
    if not need_formatting:
        print("📋 Структура листа не изменилась, пропускаю форматирование")
        return
    
    print("🎨 Применяю все форматирование одним batch-запросом")
    
    try:
        # Стандартные ширины колонок
        NARROW_WIDTH = 80   # Для оценок (цифры)
        MEDIUM_WIDTH = 150  # Для базовых полей
        WIDE_WIDTH = 250    # Для текстовых описаний
        
        # Функция для получения буквы колонки
        def get_column_letter(col_num):
            if col_num <= 26:
                return chr(64 + col_num)
            else:
                first_letter = chr(64 + ((col_num - 1) // 26))
                second_letter = chr(64 + ((col_num - 1) % 26) + 1)
                return first_letter + second_letter
        
        # 1. Обновляем заголовки с учетом объединений
        updated_headers = headers.copy()
        
        for info in criterion_headers_info:
            if info['type'] == 'merged':
                start_col = info['start_col']
                end_col = info['end_col']
                criterion_name = info['name']
                
                updated_headers[start_col] = criterion_name  # Первая колонка - название критерия
                updated_headers[end_col] = ''  # Вторая колонка - пустая для объединения
        
        # Обновляем заголовки
        worksheet.update('1:1', [updated_headers])
        print(f"📝 Обновлены заголовки: {len(updated_headers)} колонок")
        
        # 2. Подготавливаем batch-запрос
        requests = []
        
        # 2.1 Устанавливаем ширину колонок
        for i, header in enumerate(headers):
            # Определяем ширину колонки
            if header in ['id', 'date', 'phone_number', 'evaluation', 'manager', 'category']:
                width = MEDIUM_WIDTH
            elif header == 'dialogue':
                width = WIDE_WIDTH * 2  # Очень широко для диалога (500px)
            elif header.endswith('_eval') or header == '':
                width = NARROW_WIDTH
            elif header.endswith('_text') or any(info['type'] == 'single' and info['col'] == i for info in criterion_headers_info):
                width = WIDE_WIDTH
            else:
                # Проверяем в criterion_headers_info для объединенных колонок
                is_text_col = any(info['type'] == 'merged' and info['start_col'] == i for info in criterion_headers_info)
                is_eval_col = any(info['type'] == 'merged' and info['end_col'] == i for info in criterion_headers_info)
                
                if is_eval_col:
                    width = NARROW_WIDTH
                elif is_text_col:
                    width = WIDE_WIDTH
                else:
                    width = MEDIUM_WIDTH
            
            # Добавляем запрос на установку ширины колонки
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {
                        "pixelSize": width
                    },
                    "fields": "pixelSize"
                }
            })
        
        # 2.2 Объединяем ячейки для критериев
        for info in criterion_headers_info:
            if info['type'] == 'merged':
                start_col = info['start_col']
                end_col = info['end_col']
                
                requests.append({
                    "mergeCells": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": start_col,
                            "endColumnIndex": end_col + 1
                        },
                        "mergeType": "MERGE_ALL"
                    }
                })
        
        # 2.3 Форматирование всех ячеек
        end_col_letter = get_column_letter(len(headers))
        
        # Форматирование данных
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": total_rows + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(headers)
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "textFormat": {
                            "fontSize": 10
                        }
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat.fontSize)"
            }
        })
        
        # Форматирование заголовков
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(headers)
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.9,
                            "green": 0.9,
                            "blue": 0.9
                        },
                        "textFormat": {
                            "bold": True,
                            "fontSize": 10
                        },
                        "horizontalAlignment": "CENTER"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
            }
        })
        
        # Специальное форматирование для колонки dialogue (без переноса по словам)
        dialogue_col_index = None
        try:
            dialogue_col_index = headers.index('dialogue')
        except ValueError:
            pass
        
        if dialogue_col_index is not None:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Пропускаем заголовки
                        "endRowIndex": total_rows + 1,
                        "startColumnIndex": dialogue_col_index,
                        "endColumnIndex": dialogue_col_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "CLIP",  # НЕ переносить по словам
                            "verticalAlignment": "TOP",
                            "textFormat": {
                                "fontSize": 10
                            }
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat.fontSize)"
                }
            })
        
        # 2.4 Закрепляем первую строку (заголовки)
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": worksheet.id,
                    "gridProperties": {
                        "frozenRowCount": 1  # Закрепляем первую строку
                    }
                },
                "fields": "gridProperties.frozenRowCount"
            }
        })
        
        # 2.5 Добавляем автофильтры
        requests.append({
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": total_rows + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(headers)
                    }
                }
            }
        })
        
        # 3. Выполняем batch-запрос
        spreadsheet = worksheet.spreadsheet
        spreadsheet.batch_update({"requests": requests})
        
        print(f"✅ Batch-форматирование применено:")
        print(f"  📏 Ширина: {len(headers)} колонок")
        print(f"  🔗 Объединений: {len([info for info in criterion_headers_info if info['type'] == 'merged'])}")
        print(f"  🎨 Диапазон: A1:{end_col_letter}{total_rows + 1}")
        print(f"  📌 Закреплена первая строка (заголовки)")
        print(f"  🔍 Добавлены автофильтры")
        
    except Exception as e:
        print(f"❌ Ошибка batch-форматирования: {e}")
        # Fallback: основные настройки
        try:
            worksheet.columns_auto_resize(0, len(headers) - 1)
            print("  🔄 Применен fallback: автоматический размер колонок")
        except Exception as fallback_e:
            print(f"  ❌ Ошибка fallback: {fallback_e}")


def get_or_create_worksheet(spreadsheet, sheet_name: str, headers: List[str]):
    """
    Получает существующий лист или создает новый с заголовками
    
    :param spreadsheet: Объект Google Spreadsheet
    :param sheet_name: Название листа
    :param headers: Список заголовков колонок
    :return: (Объект Worksheet, is_new_sheet)
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Лист '{sheet_name}' найден")
        return worksheet, False  # Существующий лист
        
    except gspread.WorksheetNotFound:
        print(f"Создаю новый лист '{sheet_name}'")
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(headers))
        worksheet.append_row(headers)
        return worksheet, True  # Новый лист


def prepare_records_data(portal_name: str, portal_data: Dict, criteria: List[Dict]) -> tuple:
    """
    Подготавливает данные записей для загрузки в Google Sheets
    
    :param portal_name: Название портала
    :param portal_data: Данные портала (records, entities, users, etc.)
    :param criteria: Список критериев
    :return: Кортеж (заголовки, строки данных, информация об объединении критериев)
    """
    records = portal_data.get('records', [])
    entities = portal_data.get('entities', [])
    users = portal_data.get('users', [])
    
    if not records:
        return [], [], []
    
    # Создаем словари для быстрого поиска
    entities_dict = {e['id']: e for e in entities}
    users_dict = {u['id']: u for u in users}
    criteria_dict = {c['id']: c for c in criteria}
    
    # Базовые заголовки
    headers = ['id', 'date', 'phone_number', 'manager', 'category', 'evaluation', 'dialogue']
    
    # Добавляем заголовки для критериев
    criterion_headers_info = []  # Для отслеживания объединений
    
    for criterion in criteria:
        show_text = criterion.get('show_text_description', False)
        show_evaluation = criterion.get('evaluate_criterion', False)
        criterion_name = criterion['name']
        
        if show_text and show_evaluation:
            # И текст И оценка - добавляем 2 колонки, потом объединим заголовок
            headers.append(f"{criterion_name}_text")  # Временное имя для текста
            headers.append(f"{criterion_name}_eval")  # Временное имя для оценки
            criterion_headers_info.append({
                'type': 'merged',
                'name': criterion_name,
                'start_col': len(headers) - 2,  # Индекс первой колонки
                'end_col': len(headers) - 1     # Индекс второй колонки
            })
        elif show_text:
            # Только текст
            headers.append(criterion_name)
            criterion_headers_info.append({
                'type': 'single',
                'name': criterion_name,
                'col': len(headers) - 1
            })
        elif show_evaluation:
            # Только оценка
            headers.append(criterion_name)
            criterion_headers_info.append({
                'type': 'single', 
                'name': criterion_name,
                'col': len(headers) - 1
            })
    
    # Сортируем записи по дате в возрастающем порядке (самые новые внизу)
    sorted_records = sorted(records, key=lambda x: x.get('date', ''), reverse=False)
    
    # Подготавливаем строки данных
    rows = []
    for record in sorted_records:
        row_data = {}
        
        # Базовые поля
        row_data['id'] = record.get('id', '')
        row_data['date'] = record.get('date', '')
        row_data['phone_number'] = record.get('phone_number', '')
        
        # Менеджер из пользователя по user_id
        user_id = record.get('user_id')
        manager_name = ''
        
        if user_id and user_id in users_dict:
            user = users_dict[user_id]
            name = user.get('name', '') or ''
            last_name = user.get('last_name', '') or ''
            manager_name = f"{name} {last_name}".strip()
        
        row_data['manager'] = manager_name
        
        # Категория из data['categories']
        data = record.get('data', {})
        categories_list = data.get('categories', [])
        category_name = categories_list[0].get('name', '') if categories_list else ''
        row_data['category'] = category_name
        
        # Вычисляем evaluation из data['criteria']
        record_criteria_list = data.get('criteria', [])
        row_data['evaluation'] = calculate_evaluation_from_record_data(record_criteria_list, criteria)
        
        # Добавляем полный текст диалога
        dialogue_text = record.get('dialogue', '') or ''
        row_data['dialogue'] = dialogue_text
        
        # Добавляем данные по критериям из data['criteria']
        record_criteria_list = data.get('criteria', [])
        
        # Создаем словарь критериев из записи по ID для быстрого поиска
        record_criteria_dict = {rc.get('id'): rc for rc in record_criteria_list}
        
        for criterion in criteria:
            criterion_id = criterion['id']
            criterion_name = criterion['name']
            show_text = criterion.get('show_text_description', False)
            show_evaluation = criterion.get('evaluate_criterion', False)
            
            # Находим данные этого критерия в записи
            record_criterion_data = record_criteria_dict.get(criterion_id, {})
            
            if show_text and show_evaluation:
                # И текст И оценка - заполняем обе временные колонки
                text_value = record_criterion_data.get('text', '')
                evaluation_value = record_criterion_data.get('evaluation', '')
                
                row_data[f"{criterion_name}_text"] = text_value
                row_data[f"{criterion_name}_eval"] = evaluation_value if evaluation_value is not None else ''
                
            elif show_text:
                # Только текст
                text_value = record_criterion_data.get('text', '')
                row_data[criterion_name] = text_value
                
            elif show_evaluation:
                # Только оценка
                evaluation_value = record_criterion_data.get('evaluation', '')
                row_data[criterion_name] = evaluation_value if evaluation_value is not None else ''
        
        # Преобразуем в список значений согласно порядку заголовков
        row_values = [str(row_data.get(header, '')) for header in headers]
        rows.append(row_values)
    
    return headers, rows, criterion_headers_info


def prepare_entities_data(portal_name: str, portal_data: Dict, criteria: List[Dict]) -> tuple:
    """
    Подготавливает данные сущностей для загрузки в Google Sheets
    
    :param portal_name: Название портала
    :param portal_data: Данные портала (records, entities, users, etc.)
    :param criteria: Список критериев
    :return: Кортеж (заголовки, строки данных)
    """
    entities = portal_data.get('entities', [])
    
    if not entities:
        return [], []
    
    # Базовые заголовки
    headers = ['id', 'crm_entity_type', 'name', 'evaluation']
    
    # Добавляем заголовки для критериев (только те, что включены в описание сущности)
    for criterion in criteria:
        if criterion.get('include_in_entity_description', False):
            if criterion.get('show_text_description', False):
                headers.append(criterion['name'])
            if criterion.get('evaluate_criterion', False):
                headers.append(f"{criterion['name']} оценка")
    
    # Сортируем сущности по ID в возрастающем порядке (новые внизу)
    sorted_entities = sorted(entities, key=lambda x: x.get('id', 0), reverse=False)
    
    # Подготавливаем строки данных
    rows = []
    for entity in sorted_entities:
        row_data = {}
        
        # Базовые поля
        row_data['id'] = entity.get('id', '')
        row_data['crm_entity_type'] = entity.get('crm_entity_type', '')
        
        # Объединяем title, name, lastname в одно поле name
        title = entity.get('title', '') or ''
        name = entity.get('name', '') or ''
        lastname = entity.get('lastname', '') or ''
        
        # Убираем None и пустые строки
        name_parts = [part for part in [title, name, lastname] if part and part != 'None']
        full_name = ' '.join(name_parts) if name_parts else 'Без имени'
        row_data['name'] = full_name
        
        # Вычисляем evaluation (только критерии с include_in_score=True и include_in_entity_description=True)
        data = entity.get('data', {})
        row_data['evaluation'] = calculate_evaluation(
            data, criteria, 
            include_in_score_only=True, 
            include_in_entity_description=True
        )
        
        # Добавляем данные по критериям (только те, что включены в описание сущности)
        for criterion in criteria:
            if not criterion.get('include_in_entity_description', False):
                continue
                
            criterion_id = str(criterion['id'])
            criterion_data = data.get(criterion_id, {})
            
            # Текстовое описание критерия
            if criterion.get('show_text_description', False):
                row_data[criterion['name']] = criterion_data.get('description', '')
            
            # Оценка критерия
            if criterion.get('evaluate_criterion', False):
                score = criterion_data.get('score', '')
                row_data[f"{criterion['name']} оценка"] = score if score is not None else ''
        
        # Преобразуем в список значений согласно порядку заголовков
        row_values = [str(row_data.get(header, '')) for header in headers]
        rows.append(row_values)
    
    return headers, rows


async def upload_to_google_sheets(data: Dict):
    """
    Основная функция загрузки данных в Google Sheets
    
    :param data: Данные из load_records_entities_and_users
    """
    print("Начинаю загрузку данных в Google Sheets")
    
    # Загружаем настройки и учетные данные
    portal_settings = load_portal_settings()
    gc = load_google_credentials()
    
    for portal_name, portal_data in data.items():
        print(f"\nОбрабатываю портал: {portal_name}")
        
        # Получаем настройки портала
        portal_config = portal_settings.get(portal_name, {})
        spreadsheet_id = portal_config.get('googlespreadsheet_id')
        
        if not spreadsheet_id:
            print(f"Не найден googlespreadsheet_id для портала {portal_name}, пропускаю")
            continue
        
        try:
            # Открываем таблицу
            spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"Открыта таблица: {spreadsheet.title}")
            
            # Получаем критерии для портала
            criteria = portal_data.get('criteria', [])
            
            # Загружаем записи в лист "Звонки"
            records_headers, records_rows, records_criterion_info = prepare_records_data(portal_name, portal_data, criteria)
            if records_headers:  # Проверяем заголовки, а не строки (лист может быть пустым)
                records_worksheet, is_new_sheet = get_or_create_worksheet(spreadsheet, "Звонки", records_headers)
                
                # НОВАЯ ЛОГИКА: Анализируем существующий лист
                print("🔍 Анализирую существующий лист 'Звонки'")
                sheet_info = analyze_existing_worksheet(records_worksheet)
                
                print(f"📊 Найдено в листе: {sheet_info['total_rows']} записей")
                print(f"📋 Существующие колонки: {len(sheet_info['existing_headers'])}")
                
                # Добавляем недостающие колонки
                final_headers, structure_changed = add_missing_columns(
                    records_worksheet, 
                    sheet_info['existing_headers'], 
                    records_headers,
                    criteria
                )
                
                # Если это новый лист, форматирование нужно применить обязательно
                need_formatting = is_new_sheet or structure_changed
                if is_new_sheet:
                    print("🆕 Новый лист - применяю полное форматирование")
                elif structure_changed:
                    print("🔄 Структура изменилась - применяю форматирование")
                else:
                    print("📋 Структура не изменилась - пропускаю форматирование")
                
                # Фильтруем только новые записи
                new_records_only = filter_new_records(
                    records_rows, 
                    sheet_info['existing_record_ids'], 
                    records_headers
                )
                
                # Добавляем новые записи в конец
                insert_new_records_at_bottom(
                    records_worksheet, 
                    new_records_only, 
                    sheet_info['existing_headers'], 
                    final_headers
                )
                
                # Применяем все форматирование одним batch-запросом
                total_data_rows = len(new_records_only) + sheet_info['total_rows']
                apply_all_formatting_batch(
                    records_worksheet, 
                    final_headers, 
                    records_criterion_info, 
                    total_data_rows, 
                    need_formatting=need_formatting
                )
            
            # Загружаем сущности в лист "Сущности"
            entities_headers, entities_rows = prepare_entities_data(portal_name, portal_data, criteria)
            if entities_headers:  # Проверяем заголовки, а не строки (лист может быть пустым)
                entities_worksheet, is_new_entities_sheet = get_or_create_worksheet(spreadsheet, "Сущности", entities_headers)
                
                # НОВАЯ ЛОГИКА: Анализируем существующий лист
                print("🔍 Анализирую существующий лист 'Сущности'")
                sheet_info = analyze_existing_worksheet(entities_worksheet)
                
                print(f"📊 Найдено в листе: {sheet_info['total_rows']} сущностей")
                print(f"📋 Существующие колонки: {len(sheet_info['existing_headers'])}")
                
                # Добавляем недостающие колонки
                final_headers, structure_changed = add_missing_columns(
                    entities_worksheet, 
                    sheet_info['existing_headers'], 
                    entities_headers,
                    criteria
                )
                
                # Если это новый лист, форматирование нужно применить обязательно
                need_formatting = is_new_entities_sheet or structure_changed
                if is_new_entities_sheet:
                    print("🆕 Новый лист сущностей - применяю полное форматирование")
                elif structure_changed:
                    print("🔄 Структура сущностей изменилась - применяю форматирование")
                else:
                    print("📋 Структура сущностей не изменилась - пропускаю форматирование")
                
                # Фильтруем только новые сущности
                new_entities_only = filter_new_records(
                    entities_rows, 
                    sheet_info['existing_record_ids'], 
                    entities_headers
                )
                
                # Добавляем новые сущности в конец
                insert_new_records_at_bottom(
                    entities_worksheet, 
                    new_entities_only, 
                    sheet_info['existing_headers'], 
                    final_headers
                )
                
                # Применяем все форматирование одним batch-запросом для сущностей
                total_entities_rows = len(new_entities_only) + sheet_info['total_rows']
                apply_all_formatting_batch(
                    entities_worksheet, 
                    final_headers, 
                    [], # У сущностей нет criterion_info для объединения
                    total_entities_rows, 
                    need_formatting=need_formatting
                )
            
            print(f"✅ Портал {portal_name} обработан успешно")
            
        except Exception as e:
            print(f"❌ Ошибка при обработке портала {portal_name}: {e}")
            continue
    
    print("\n✅ Загрузка в Google Sheets завершена")


async def main():
    """
    Тестовая функция для проверки загрузки
    """
    print("=== Google Sheets Uploader Test ===")
    
    # Импортируем функцию загрузки данных
    from google_sheet.db_records_loader import load_records_entities_and_users
    
    # Загружаем данные из БД
    data = await load_records_entities_and_users()
    
    # Сохраняем для отладки
    save_debug_json(data, "google_sheets_uploader_input")
    
    # Загружаем в Google Sheets
    await upload_to_google_sheets(data)
    
    print("🎉 Тест завершен")


if __name__ == "__main__":
    asyncio.run(main())

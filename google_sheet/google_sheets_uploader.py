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
    
    return sum(scores) / len(scores) if scores else 0.0


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
    
    return sum(scores) / len(scores) if scores else 0.0


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


def add_missing_columns(worksheet, existing_headers: List[str], new_headers: List[str]):
    """
    Добавляет недостающие колонки в конец листа
    
    :param worksheet: Объект Worksheet
    :param existing_headers: Существующие заголовки
    :param new_headers: Новые заголовки (полный список)
    """
    missing_headers = []
    
    # Находим недостающие заголовки
    for header in new_headers:
        if header not in existing_headers:
            missing_headers.append(header)
    
    if missing_headers:
        print(f"Добавляю {len(missing_headers)} новых колонок: {missing_headers}")
        
        # Обновляем заголовки
        updated_headers = existing_headers + missing_headers
        
        # Расширяем лист если нужно больше колонок
        if len(updated_headers) > worksheet.col_count:
            worksheet.add_cols(len(updated_headers) - worksheet.col_count)
        
        # Обновляем первую строку с заголовками
        worksheet.update(f'1:{len(updated_headers)}', [updated_headers])
        
        return updated_headers
    else:
        print("Новых колонок не требуется")
        return existing_headers


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


def get_or_create_worksheet(spreadsheet, sheet_name: str, headers: List[str]):
    """
    Получает существующий лист или создает новый с заголовками
    
    :param spreadsheet: Объект Google Spreadsheet
    :param sheet_name: Название листа
    :param headers: Список заголовков колонок
    :return: Объект Worksheet
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Лист '{sheet_name}' найден")
        return worksheet
        
    except gspread.WorksheetNotFound:
        print(f"Создаю новый лист '{sheet_name}'")
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(headers))
        worksheet.append_row(headers)
        return worksheet


def prepare_records_data(portal_name: str, portal_data: Dict, criteria: List[Dict]) -> tuple:
    """
    Подготавливает данные записей для загрузки в Google Sheets
    
    :param portal_name: Название портала
    :param portal_data: Данные портала (records, entities, users, etc.)
    :param criteria: Список критериев
    :return: Кортеж (заголовки, строки данных)
    """
    records = portal_data.get('records', [])
    entities = portal_data.get('entities', [])
    users = portal_data.get('users', [])
    
    if not records:
        return [], []
    
    # Создаем словари для быстрого поиска
    entities_dict = {e['id']: e for e in entities}
    users_dict = {u['id']: u for u in users}
    criteria_dict = {c['id']: c for c in criteria}
    
    # Базовые заголовки
    headers = ['id', 'date', 'phone_number', 'manager', 'category', 'evaluation']
    
    # Добавляем заголовки для критериев
    for criterion in criteria:
        if criterion.get('show_text_description', False):
            headers.append(criterion['name'])
        if criterion.get('evaluate_criterion', False):
            headers.append(f"{criterion['name']} оценка")
    
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
        
        # Добавляем данные по критериям из data['criteria']
        record_criteria_list = data.get('criteria', [])
        
        # Создаем словарь критериев из записи по ID для быстрого поиска
        record_criteria_dict = {rc.get('id'): rc for rc in record_criteria_list}
        
        for criterion in criteria:
            criterion_id = criterion['id']
            criterion_name = criterion['name']
            
            # Находим данные этого критерия в записи
            record_criterion_data = record_criteria_dict.get(criterion_id, {})
            
            # Текстовое описание критерия - заполняем только если show_text_description=True
            if criterion.get('show_text_description', False):
                text_value = record_criterion_data.get('text', '')
                row_data[criterion_name] = text_value
            
            # Оценка критерия - заполняем только если evaluate_criterion=True
            if criterion.get('evaluate_criterion', False):
                evaluation_value = record_criterion_data.get('evaluation', '')
                row_data[f"{criterion_name} оценка"] = evaluation_value if evaluation_value is not None else ''
        
        # Преобразуем в список значений согласно порядку заголовков
        row_values = [str(row_data.get(header, '')) for header in headers]
        rows.append(row_values)
    
    return headers, rows


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
            records_headers, records_rows = prepare_records_data(portal_name, portal_data, criteria)
            if records_headers:  # Проверяем заголовки, а не строки (лист может быть пустым)
                records_worksheet = get_or_create_worksheet(spreadsheet, "Звонки", records_headers)
                
                # НОВАЯ ЛОГИКА: Анализируем существующий лист
                print("🔍 Анализирую существующий лист 'Звонки'")
                sheet_info = analyze_existing_worksheet(records_worksheet)
                
                print(f"📊 Найдено в листе: {sheet_info['total_rows']} записей")
                print(f"📋 Существующие колонки: {len(sheet_info['existing_headers'])}")
                
                # Добавляем недостающие колонки
                final_headers = add_missing_columns(
                    records_worksheet, 
                    sheet_info['existing_headers'], 
                    records_headers
                )
                
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
            
            # Загружаем сущности в лист "Сущности"
            entities_headers, entities_rows = prepare_entities_data(portal_name, portal_data, criteria)
            if entities_headers:  # Проверяем заголовки, а не строки (лист может быть пустым)
                entities_worksheet = get_or_create_worksheet(spreadsheet, "Сущности", entities_headers)
                
                # НОВАЯ ЛОГИКА: Анализируем существующий лист
                print("🔍 Анализирую существующий лист 'Сущности'")
                sheet_info = analyze_existing_worksheet(entities_worksheet)
                
                print(f"📊 Найдено в листе: {sheet_info['total_rows']} сущностей")
                print(f"📋 Существующие колонки: {len(sheet_info['existing_headers'])}")
                
                # Добавляем недостающие колонки
                final_headers = add_missing_columns(
                    entities_worksheet, 
                    sheet_info['existing_headers'], 
                    entities_headers
                )
                
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

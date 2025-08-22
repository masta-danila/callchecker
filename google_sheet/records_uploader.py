import json
import gspread_asyncio
import gspread
from google.oauth2.service_account import Credentials
from typing import Dict, List, Tuple
import os
import asyncio

# Настройки для Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'bitrix24/google_sheets_credentials.json')

async def get_google_sheets_client():
    """Инициализирует и возвращает асинхронный клиент для работы с Google Sheets."""
    agcm = gspread_asyncio.AsyncioGspreadClientManager(
        lambda: Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    )
    return await agcm.authorize()

def load_portal_configs() -> List[Dict]:
    """Загружает конфигурации порталов из bitrix24/bitrix_portals.json."""
    with open('bitrix24/bitrix_portals.json', 'r') as f:
        data = json.load(f)
        return data.get('portals', [])

def calculate_evaluation_from_record_data(record_criteria_data: List[Dict], criteria_metadata: List[Dict]) -> float:
    """
    Вычисляет среднюю оценку для записи на основе критериев с evaluate_criterion=True и include_in_score=True.
    
    :param record_criteria_data: Данные критериев из записи (data['criteria'])
    :param criteria_metadata: Метаданные критериев из таблицы criteria
    :return: Средняя оценка, округленная до десятых
    """
    evaluations = []
    
    # Создаем словарь метаданных критериев по ID
    criteria_meta_dict = {c['id']: c for c in criteria_metadata}
    
    for record_criterion in record_criteria_data:
        criterion_id = record_criterion.get('id')
        criterion_meta = criteria_meta_dict.get(criterion_id, {})
        
        # Проверяем условия из метаданных критерия
        if (criterion_meta.get('evaluate_criterion', False) and 
            criterion_meta.get('include_in_score', False)):
            
            eval_value = record_criterion.get('evaluation')
            if isinstance(eval_value, (int, float)):
                evaluations.append(eval_value)
    
    return round(sum(evaluations) / len(evaluations), 1) if evaluations else 0.0

def prepare_records_data(portal_data: Dict, spreadsheet_id: str = '', entities_sheet_gid: str = '0', entities_rows_map: Dict = None) -> Tuple[List[str], List[List]]:
    """Подготавливает данные записей для загрузки в Google Sheets."""
    records = portal_data.get('records', [])
    users = portal_data.get('users', [])
    entities = portal_data.get('entities', [])
    criteria = portal_data.get('criteria', [])

    if not records:
        return [], []

    # Создаем словари для быстрого поиска
    users_dict = {u['id']: u for u in users}
    entities_dict = {e['id']: e for e in entities}

    # Базовые заголовки
    headers = ['id', 'date', 'phone_number', 'manager', 'entity_name', 'category', 'evaluation', 'dialogue', 'summary']

    # Добавляем заголовки для критериев
    for criterion in criteria:
        criterion_name = criterion['name']
        headers.append(criterion_name)  # Колонка с названием критерия
        if criterion.get('evaluate_criterion', False):
            headers.append('')  # Пустой заголовок для колонки оценки

    # Сортируем записи по дате (самые новые внизу)
    sorted_records = sorted(records, key=lambda x: x.get('date', ''), reverse=False)

    # Подготавливаем строки данных
    rows = []
    for record in sorted_records:
        row = []
        
        # Базовые поля
        row.append(record.get('id', ''))
        row.append(record.get('date', ''))
        row.append(record.get('phone_number', ''))
        
        # Менеджер из пользователя по user_id
        user_id = record.get('user_id')
        manager_name = ''
        if user_id and user_id in users_dict:
            user = users_dict[user_id]
            name = user.get('name', '') or ''
            last_name = user.get('last_name', '') or ''
            manager_name = f"{name} {last_name}".strip()
        row.append(manager_name)
        
        # Имя сущности с гиперссылкой на лист "Сущности" (title + name + lastname)
        entity_id = record.get('entity_id')
        entity_name = ''
        if entity_id and entity_id in entities_dict:
            entity = entities_dict[entity_id]
            title = entity.get('title', '') or ''
            name = entity.get('name', '') or ''
            lastname = entity.get('lastname', '') or ''
            name_parts = [part for part in [title, name, lastname] if part and part != 'None']
            display_name = ' '.join(name_parts) if name_parts else f'Сущность {entity_id}'
            
            # Создаем гиперссылку на соответствующую строку в листе "Сущности"
            if spreadsheet_id and entities_rows_map and str(entity_id) in entities_rows_map:
                # Полная ссылка на конкретную строку сущности в Google Sheets
                entity_row = entities_rows_map[str(entity_id)]
                full_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={entities_sheet_gid}&range=A{entity_row}"
                entity_name = {"formula": f'=HYPERLINK("{full_url}"; "{display_name}")'}
            else:
                entity_name = display_name
        row.append(entity_name)
        
        # Категория из data['categories']
        data = record.get('data', {})
        categories_list = data.get('categories', [])
        category_name = categories_list[0].get('name', '') if categories_list else ''
        row.append(category_name)
        
        # Вычисляем evaluation
        record_criteria_list = data.get('criteria', [])
        evaluation = calculate_evaluation_from_record_data(record_criteria_list, criteria)
        row.append(evaluation)
        
        # Текст диалога
        row.append(record.get('dialogue', '') or '')
        
        # Краткое резюме
        row.append(record.get('summary', '') or '')
        
        # Добавляем данные по критериям
        record_criteria_dict = {rc.get('id'): rc for rc in record_criteria_list}
        
        for criterion in criteria:
            criterion_id = criterion['id']
            record_criterion_data = record_criteria_dict.get(criterion_id, {})
            
            # Текст критерия (если показывать)
            text_value = record_criterion_data.get('text', '') if criterion.get('show_text_description', False) else ''
            row.append(text_value)
            
            # Оценка критерия (если есть)
            if criterion.get('evaluate_criterion', False):
                eval_value = record_criterion_data.get('evaluation', '')
                row.append(eval_value if eval_value is not None else '')
        
        rows.append(row)

    return headers, rows

async def get_or_create_worksheet(spreadsheet, sheet_name: str):
    """Получает или создает лист с указанным именем."""
    try:
        worksheet = await spreadsheet.worksheet(sheet_name)
        print(f"📄 Лист '{sheet_name}' найден")
        return worksheet
    except gspread.exceptions.WorksheetNotFound:
        worksheet = await spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=50)
        print(f"📄 Лист '{sheet_name}' создан")
        return worksheet

async def analyze_existing_worksheet(worksheet) -> Dict:
    """Анализирует существующий лист на наличие данных и заголовков."""
    all_values = await worksheet.get_all_values()
    if not all_values:
        return {'is_empty': True, 'existing_headers': [], 'existing_data': []}

    existing_headers = all_values[0] if all_values else []
    existing_data = all_values[1:] if len(all_values) > 1 else []
    
    return {
        'is_empty': len(existing_data) == 0,
        'existing_headers': existing_headers,
        'existing_data': existing_data
    }

def filter_new_records(new_rows: List[List], existing_data: List[List]) -> List[List]:
    """Фильтрует новые записи, исключая те, которые уже есть в таблице (по ID)."""
    if not existing_data:
        return new_rows

    existing_ids = {row[0] for row in existing_data if row and len(row) > 0}
    filtered_rows = []

    for row in new_rows:
        record_id = row[0] if row else None
        if record_id and record_id in existing_ids:
            continue
        else:
            filtered_rows.append(row)

    original_count = len(new_rows)
    filtered_count = len(filtered_rows)
    duplicate_count = original_count - filtered_count
    
    print(f"🔍 Фильтрация записей: {original_count} всего, {filtered_count} новых, {duplicate_count} дубликатов")
    return filtered_rows

async def apply_header_and_formatting(worksheet, headers: List[str], spreadsheet):
    """Применяет заголовки и форматирование к листу."""
    
    # Обновляем заголовки
    await worksheet.update('1:1', [headers])
    
    # Подготавливаем batch-запрос для форматирования
    requests = []
    
    # 1. Форматирование шапки (жирный шрифт, заливка #e5e5e5)
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
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                    "textFormat": {"bold": True}
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })
    
    # 2. Закрепление первой строки
    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": worksheet.id,
                "gridProperties": {
                    "frozenRowCount": 1
                }
            },
            "fields": "gridProperties.frozenRowCount"
        }
    })
    
    # 3. Установка автофильтра
    requests.append({
        "setBasicFilter": {
            "filter": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,  # Достаточно большой диапазон
                    "startColumnIndex": 0,
                    "endColumnIndex": len(headers)
                }
            }
        }
    })
    
    # 4. Выравнивание по левому краю, верхнему краю и перенос по строкам для всей таблицы
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": 0,
                "endRowIndex": 1000,  # Достаточно большой диапазон
                "startColumnIndex": 0,
                "endColumnIndex": len(headers)
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "LEFT",
                    "verticalAlignment": "TOP",
                    "wrapStrategy": "WRAP"
                }
            },
            "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy)"
        }
    })
    
    # 5. Установка ширины колонок
    column_widths = {
        'id': 150,
        'date': 150,
        'phone_number': 150,
        'manager': 150,
        'entity_name': 150,
        'category': 150,
        'evaluation': 150,
        'dialogue': 500,
        'summary': 250
    }
    
    for i, header in enumerate(headers):
        if header == '':
            # Колонка оценки без названия
            width = 50
        else:
            # Обычная колонка
            width = column_widths.get(header, 250)  # По умолчанию 250 для колонок критериев
        
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
    
    # 6. Отключение переноса по словам для колонки dialogue
    dialogue_index = headers.index('dialogue') if 'dialogue' in headers else -1
    if dialogue_index >= 0:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,  # Достаточно большой диапазон
                    "startColumnIndex": dialogue_index,
                    "endColumnIndex": dialogue_index + 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "OVERFLOW_CELL"
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy)"
            }
        })
    
    # Выполняем batch-запрос
    await spreadsheet.batch_update({"requests": requests})
    print("🎨 Форматирование применено: шапка, ширина колонок, фильтры, выравнивание")

async def insert_records_batch(worksheet, rows: List[List], spreadsheet):
    """Добавляет записи в лист одним батчем, сохраняя типы данных."""
    if not rows:
        print("📝 Нет записей для добавления")
        return
    all_values = await worksheet.get_all_values()
    start_row = len(all_values) + 1 if all_values else 1

    # Расширяем лист, если нужно
    needed_rows = start_row + len(rows) - 1
    if needed_rows > worksheet.row_count:
        await worksheet.add_rows(needed_rows - worksheet.row_count)

    requests = []
    for row_idx, row in enumerate(rows):
        current_row = start_row + row_idx
        for col_idx, cell_value in enumerate(row):
            # Определяем тип значения для корректной записи
            user_value = {}
            if isinstance(cell_value, dict) and 'formula' in cell_value:
                # Обрабатываем формулы (например, гиперссылки)
                user_value = {"formulaValue": cell_value['formula']}
            elif isinstance(cell_value, (int, float)):
                user_value = {"numberValue": float(cell_value)}
            elif cell_value is None or cell_value == '':
                user_value = {"stringValue": ""}
            else:
                user_value = {"stringValue": str(cell_value)}

            requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": current_row - 1,  # 0-based
                        "endRowIndex": current_row,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "rows": [{
                        "values": [{
                            "userEnteredValue": user_value
                        }]
                    }],
                    "fields": "userEnteredValue"
                }
            })

    # Выполняем batch-запрос по частям (лимит API)
    if requests:
        chunk_size = 100  # Лимит API
        for i in range(0, len(requests), chunk_size):
            chunk = requests[i:i + chunk_size]
            await spreadsheet.batch_update({"requests": chunk})
        
        print(f"✅ Добавлено {len(rows)} записей через batch API")

async def get_entities_rows_map(spreadsheet, sheet_name: str) -> Dict[str, int]:
    """Получает карту ID сущностей к номерам строк в листе."""
    try:
        worksheet = await spreadsheet.worksheet(sheet_name)
        all_values = await worksheet.get_all_values()
        
        entities_map = {}
        if len(all_values) > 1:  # Пропускаем заголовок
            for row_idx, row in enumerate(all_values[1:], start=2):  # Начинаем с строки 2
                if row and len(row) > 0:
                    entity_id = row[0]  # ID в первой колонке
                    entities_map[entity_id] = row_idx
        
        print(f"🗺️  Создана карта строк для {len(entities_map)} сущностей")
        return entities_map
    except Exception as e:
        print(f"⚠️  Не удалось создать карту строк сущностей: {e}")
        return {}

async def upload_records_to_google_sheets(portal_data: Dict, portal_config: Dict):
    """Основная функция для загрузки записей в Google Sheets."""
    portal_url = portal_config.get('url', 'Unknown')
    spreadsheet_id = portal_config.get('googlespreadsheet_id', '')
    
    if not spreadsheet_id:
        print(f"⚠️  Для портала {portal_url} не указан ID Google Sheets. Пропускаю.")
        return

    print(f"📊 Загружаю данные для портала {portal_url} в Google Sheets...")

    # Подключаемся к Google Sheets
    client = await get_google_sheets_client()
    spreadsheet = await client.open_by_key(spreadsheet_id)

    # Получаем gid листа "Сущности" и карту строк для гиперссылок
    entities_sheet_gid = '0'  # По умолчанию
    entities_rows_map = {}
    try:
        entities_worksheet = await spreadsheet.worksheet("Сущности")
        entities_sheet_gid = str(entities_worksheet.id)
        # Получаем карту строк сущностей для точных ссылок
        entities_rows_map = await get_entities_rows_map(spreadsheet, "Сущности")
    except:
        pass  # Лист "Сущности" может еще не существовать

    # Пересоздаем данные с правильным gid и картой строк для гиперссылок
    headers, rows = prepare_records_data(portal_data, spreadsheet_id, entities_sheet_gid, entities_rows_map)
    if not rows:
        print("📝 Нет записей для загрузки")
        return

    # Получаем или создаем лист "Звонки"
    worksheet = await get_or_create_worksheet(spreadsheet, "Звонки")

    # Анализируем существующий лист
    sheet_info = await analyze_existing_worksheet(worksheet)
    existing_headers = sheet_info['existing_headers']
    existing_data = sheet_info['existing_data']
    is_new_sheet = sheet_info['is_empty'] and not existing_headers

    if is_new_sheet:
        # Новый лист: создаем шапку и загружаем все данные
        await apply_header_and_formatting(worksheet, headers, spreadsheet)
        await insert_records_batch(worksheet, rows, spreadsheet)
        print(f"🎉 Новый лист 'Звонки' создан и заполнен {len(rows)} записями")
    else:
        # Существующий лист: проверяем заголовки и добавляем недостающие записи
        if existing_headers != headers:
            await apply_header_and_formatting(worksheet, headers, spreadsheet)
            print("🔄 Заголовки обновлены, форматирование применено")
        else:
            print("✅ Заголовки не изменились, форматирование не требуется")

        # Фильтруем новые записи
        filtered_rows = filter_new_records(rows, existing_data)
        if filtered_rows:
            await insert_records_batch(worksheet, filtered_rows, spreadsheet)
            print(f"✅ Добавлено {len(filtered_rows)} новых записей в существующий лист")
        else:
            print("📝 Новых записей для добавления нет")

async def upload_to_google_sheets(all_portals_data: Dict):
    """Обрабатывает загрузку данных для всех порталов параллельно."""
    portals = load_portal_configs()
    tasks = []
    
    for portal_config in portals:
        # Определяем название портала по URL
        portal_url = portal_config.get('url', '')
        portal_name = None
        
        # Ищем соответствующие данные для этого портала
        for name, data in all_portals_data.items():
            if name in portal_url or portal_url.endswith(name):
                portal_name = name
                break
        
        if portal_name and portal_name in all_portals_data:
            portal_data = all_portals_data[portal_name]
            task = upload_records_to_google_sheets(portal_data, portal_config)
            tasks.append(task)
        else:
            print(f"⚠️  Данные для портала {portal_url} не найдены")
    
    # Выполняем все задачи параллельно
    if tasks:
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Тестовые данные
    test_data = {
        "records": [],
        "users": [],
        "criteria": []
    }
    asyncio.run(upload_to_google_sheets(test_data))

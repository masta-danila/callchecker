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

def calculate_evaluation_from_entity_data(entity_criteria_data: List[Dict], criteria_metadata: List[Dict]) -> float:
    """
    Вычисляет среднюю оценку для сущности на основе критериев с evaluate_criterion=True и include_in_score=True.
    
    :param entity_criteria_data: Данные критериев из сущности (data['criteria'])
    :param criteria_metadata: Метаданные критериев из таблицы criteria
    :return: Средняя оценка, округленная до десятых
    """
    evaluations = []
    
    # Создаем словарь метаданных критериев по ID
    criteria_meta_dict = {c['id']: c for c in criteria_metadata}
    
    for entity_criterion in entity_criteria_data:
        criterion_id = entity_criterion.get('id')
        criterion_meta = criteria_meta_dict.get(criterion_id, {})
        
        # Проверяем условия из метаданных критерия
        if (criterion_meta.get('evaluate_criterion', False) and 
            criterion_meta.get('include_in_score', False) and
            criterion_meta.get('include_in_entity_description', False)):
            
            eval_value = entity_criterion.get('evaluation')
            if isinstance(eval_value, (int, float)):
                evaluations.append(eval_value)
    
    return round(sum(evaluations) / len(evaluations), 1) if evaluations else 0.0

def prepare_entities_data(portal_data: Dict) -> Tuple[List[str], List[List]]:
    """Подготавливает данные сущностей для загрузки в Google Sheets."""
    entities = portal_data.get('entities', [])
    criteria = portal_data.get('criteria', [])

    if not entities:
        return [], []

    # Базовые заголовки
    headers = ['id', 'crm_entity_type', 'name', 'evaluation', 'summary']

    # Добавляем заголовки для критериев (только с include_in_entity_description=True)
    entity_criteria = [c for c in criteria if c.get('include_in_entity_description', False)]
    for criterion in entity_criteria:
        criterion_name = criterion['name']
        headers.append(criterion_name)  # Колонка с названием критерия
        if criterion.get('evaluate_criterion', False):
            headers.append('')  # Пустой заголовок для колонки оценки

    # Сортируем сущности по ID
    sorted_entities = sorted(entities, key=lambda x: x.get('id', 0), reverse=False)

    # Подготавливаем строки данных
    rows = []
    for entity in sorted_entities:
        row = []
        
        # Базовые поля (приводим ID к строке для совместимости с Google Sheets)
        row.append(str(entity.get('id', '')))
        row.append(entity.get('crm_entity_type', ''))
        
        # Имя сущности (title + name + lastname)
        title = entity.get('title', '') or ''
        name = entity.get('name', '') or ''
        lastname = entity.get('lastname', '') or ''
        name_parts = [part for part in [title, name, lastname] if part and part != 'None']
        entity_full_name = ' '.join(name_parts) if name_parts else f'Сущность {entity.get("id", "")}'
        row.append(entity_full_name)
        
        # Вычисляем evaluation
        entity_criteria_list = entity.get('data', {}).get('criteria', [])
        evaluation = calculate_evaluation_from_entity_data(entity_criteria_list, criteria)
        row.append(evaluation)
        
        # Краткое резюме
        row.append(entity.get('summary', '') or '')
        
        # Добавляем данные по критериям (только с include_in_entity_description=True)
        entity_criteria_dict = {ec.get('id'): ec for ec in entity_criteria_list}
        
        for criterion in entity_criteria:
            criterion_id = criterion['id']
            entity_criterion_data = entity_criteria_dict.get(criterion_id, {})
            
            # Текст критерия (если показывать)
            text_value = entity_criterion_data.get('text', '') if criterion.get('show_text_description', False) else ''
            row.append(text_value)
            
            # Оценка критерия (если есть)
            if criterion.get('evaluate_criterion', False):
                eval_value = entity_criterion_data.get('evaluation', '')
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

def filter_new_entities(new_rows: List[List], existing_data: List[List]) -> List[List]:
    """Фильтрует новые сущности, исключая те, которые уже есть в таблице (по ID)."""
    if not existing_data:
        return new_rows

    existing_ids = {row[0] for row in existing_data if row and len(row) > 0}
    filtered_rows = []

    for row in new_rows:
        entity_id = row[0] if row else None
        if entity_id and entity_id in existing_ids:
            continue
        else:
            filtered_rows.append(row)

    original_count = len(new_rows)
    filtered_count = len(filtered_rows)
    duplicate_count = original_count - filtered_count
    
    print(f"🔍 Фильтрация сущностей: {original_count} всего, {filtered_count} новых, {duplicate_count} дубликатов")
    return filtered_rows

def find_updated_entities(new_rows: List[List], existing_data: List[List]) -> List[Tuple[int, List]]:
    """Находит обновленные сущности для построчного обновления."""
    if not existing_data:
        return []

    # Создаем словарь существующих данных по ID
    existing_dict = {}
    for i, row in enumerate(existing_data):
        if row and len(row) > 0:
            existing_dict[row[0]] = (i + 2, row)  # +2 для учета заголовков и 1-based индексации

    updated_entities = []
    for new_row in new_rows:
        entity_id = new_row[0] if new_row else None
        if entity_id and entity_id in existing_dict:
            row_index, existing_row = existing_dict[entity_id]
            # Проверяем, изменились ли данные
            if new_row != existing_row:
                updated_entities.append((row_index, new_row))

    print(f"🔄 Найдено {len(updated_entities)} обновленных сущностей")
    return updated_entities

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
                    "endRowIndex": 1000,
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
                "endRowIndex": 1000,
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
        'crm_entity_type': 150,
        'name': 150,
        'evaluation': 150,
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
    
    # Выполняем batch-запрос
    await spreadsheet.batch_update({"requests": requests})
    print("🎨 Форматирование применено: шапка, ширина колонок, фильтры, выравнивание")

async def insert_entities_batch(worksheet, rows: List[List], spreadsheet):
    """Добавляет сущности в лист одним батчем, сохраняя типы данных."""
    if not rows:
        print("📝 Нет сущностей для добавления")
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
            if isinstance(cell_value, (int, float)):
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
        
        print(f"✅ Добавлено {len(rows)} сущностей через batch API")

async def update_entities_row_by_row(worksheet, updated_entities: List[Tuple[int, List]], spreadsheet):
    """Обновляет существующие сущности построчно."""
    if not updated_entities:
        print("🔄 Нет сущностей для обновления")
        return

    print(f"🔄 Обновляю {len(updated_entities)} сущностей построчно...")
    
    for row_index, row_data in updated_entities:
        requests = []
        
        # Обновляем всю строку за один запрос
        for col_idx, cell_value in enumerate(row_data):
            user_value = {}
            if isinstance(cell_value, (int, float)):
                user_value = {"numberValue": float(cell_value)}
            elif cell_value is None or cell_value == '':
                user_value = {"stringValue": ""}
            else:
                user_value = {"stringValue": str(cell_value)}

            requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": row_index - 1,  # 0-based
                        "endRowIndex": row_index,
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
        
        # Выполняем запрос для одной строки
        if requests:
            await spreadsheet.batch_update({"requests": requests})
    
    print(f"✅ Обновлено {len(updated_entities)} сущностей")

async def upload_entities_to_google_sheets(portal_data: Dict, portal_config: Dict):
    """Основная функция для загрузки сущностей в Google Sheets."""
    portal_url = portal_config.get('url', 'Unknown')
    spreadsheet_id = portal_config.get('googlespreadsheet_id', '')
    
    if not spreadsheet_id:
        print(f"⚠️  Для портала {portal_url} не указан ID Google Sheets. Пропускаю.")
        return

    print(f"📊 Загружаю сущности для портала {portal_url} в Google Sheets...")

    # Подготавливаем данные
    headers, rows = prepare_entities_data(portal_data)
    if not rows:
        print("📝 Нет сущностей для загрузки")
        return

    # Подключаемся к Google Sheets
    client = await get_google_sheets_client()
    spreadsheet = await client.open_by_key(spreadsheet_id)

    # Получаем или создаем лист "Сущности"
    worksheet = await get_or_create_worksheet(spreadsheet, "Сущности")

    # Анализируем существующий лист
    sheet_info = await analyze_existing_worksheet(worksheet)
    existing_headers = sheet_info['existing_headers']
    existing_data = sheet_info['existing_data']
    is_new_sheet = sheet_info['is_empty'] and not existing_headers

    if is_new_sheet:
        # Новый лист: создаем шапку и загружаем все данные
        await apply_header_and_formatting(worksheet, headers, spreadsheet)
        await insert_entities_batch(worksheet, rows, spreadsheet)
        print(f"🎉 Новый лист 'Сущности' создан и заполнен {len(rows)} сущностями")
    else:
        # Существующий лист: проверяем заголовки, обновляем существующие и добавляем новые
        if existing_headers != headers:
            await apply_header_and_formatting(worksheet, headers, spreadsheet)
            print("🔄 Заголовки обновлены, форматирование применено")
        else:
            print("✅ Заголовки не изменились, форматирование не требуется")

        # Находим обновленные сущности и обновляем их построчно
        updated_entities = find_updated_entities(rows, existing_data)
        if updated_entities:
            await update_entities_row_by_row(worksheet, updated_entities, spreadsheet)

        # Фильтруем новые сущности и добавляем их батчем
        filtered_rows = filter_new_entities(rows, existing_data)
        if filtered_rows:
            await insert_entities_batch(worksheet, filtered_rows, spreadsheet)
            print(f"✅ Добавлено {len(filtered_rows)} новых сущностей в существующий лист")
        else:
            print("📝 Новых сущностей для добавления нет")

async def upload_entities_to_google_sheets_all_portals(all_portals_data: Dict):
    """Обрабатывает загрузку сущностей для всех порталов параллельно."""
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
            task = upload_entities_to_google_sheets(portal_data, portal_config)
            tasks.append(task)
        else:
            print(f"⚠️  Данные для портала {portal_url} не найдены")
    
    # Выполняем все задачи параллельно
    if tasks:
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Тестовые данные
    test_data = {
        "entities": [],
        "criteria": []
    }
    asyncio.run(upload_entities_to_google_sheets_all_portals(test_data))

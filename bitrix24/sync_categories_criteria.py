import os
import sys
import json
import gspread
from google.oauth2.service_account import Credentials

# Добавляем корневую папку в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import get_db_client


def get_google_sheets_client():
    """Авторизация в Google Sheets API"""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials_path = os.path.join(os.path.dirname(__file__), "google_sheets_credentials.json")
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    return gspread.authorize(credentials)


def load_portals_config():
    """Загружаем конфигурацию порталов"""
    config_path = os.path.join(os.path.dirname(__file__), "bitrix_portals.json")
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_portal_name(portal_url):
    """Извлекаем имя портала из URL"""
    try:
        return portal_url.split('/')[2].split('.')[0]
    except:
        return None


def sync_criterion_groups(portal_name, spreadsheet_id):
    """Синхронизация групп критериев между Google Sheets и БД"""
    print(f"Синхронизация групп критериев для портала '{portal_name}'")
    
    # Подключаемся к Google Sheets
    gc = get_google_sheets_client()
    sheet = gc.open_by_key(spreadsheet_id)
    worksheet = sheet.worksheet("Группы критериев")
    sheet_data = worksheet.get_all_records()
    
    if not sheet_data:
        print("Нет данных в листе 'Группы критериев'")
        return
    
    # Работаем с БД
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            table_name = f"{portal_name}_criterion_groups"
            updated_data = []
            
            for row in sheet_data:
                group_id = str(row.get('id', '')).strip()
                group_name = str(row.get('name', '')).strip()
                
                if not group_name:
                    continue
                
                if group_id and group_id.isdigit():
                    # Обновляем существующую запись
                    cursor.execute(f"""
                        UPDATE {table_name} 
                        SET name = %s 
                        WHERE id = %s
                        RETURNING id;
                    """, (group_name, int(group_id)))
                    
                    result = cursor.fetchone()
                    if result:
                        updated_data.append({'id': str(result[0]), 'name': group_name})
                        print(f"Обновлена группа ID {group_id}: {group_name}")
                else:
                    # Создаем новую запись
                    cursor.execute(f"""
                        INSERT INTO {table_name} (name) 
                        VALUES (%s) 
                        RETURNING id;
                    """, (group_name,))
                    
                    new_id = cursor.fetchone()[0]
                    updated_data.append({'id': str(new_id), 'name': group_name})
                    print(f"Создана новая группа ID {new_id}: {group_name}")
            
            conn.commit()
            
            # Обновляем Google Sheets с новыми ID (только колонку ID)
            if updated_data:
                try:
                    # Пытаемся обновить только колонку A (ID)
                    id_values = []
                    for row in updated_data:
                        id_values.append([row['id']])
                    
                    # Обновляем колонку A, начиная со второй строки (пропуская заголовок)
                    worksheet.update(f'A2:A{len(updated_data)+1}', id_values, value_input_option='RAW')
                    print(f"Обновлены ID групп критериев в Google Sheets")
                except Exception as e:
                    print(f"Не удалось обновить Google Sheets: {e}")
                    print("ID групп критериев сохранены в БД, но не обновлены в таблице")
                
                print(f"Синхронизация групп критериев завершена для портала '{portal_name}'")


def sync_criteria(portal_name, spreadsheet_id):
    """Синхронизация критериев между Google Sheets и БД"""
    print(f"Синхронизация критериев для портала '{portal_name}'")
    
    # Подключаемся к Google Sheets
    gc = get_google_sheets_client()
    sheet = gc.open_by_key(spreadsheet_id)
    worksheet = sheet.worksheet("Критерии")
    
    # Читаем все данные как списки
    all_values = worksheet.get_all_values()
    if not all_values:
        print("Нет данных в листе 'Критерии'")
        return
    
    # Первая строка - заголовки
    headers = all_values[0]
    sheet_data = []
    
    # Преобразуем в список словарей
    for row in all_values[1:]:
        row_dict = {}
        for i, header in enumerate(headers):
            if i < len(row):
                row_dict[header] = row[i]
            else:
                row_dict[header] = ''
        sheet_data.append(row_dict)
    
    if not sheet_data:
        print("Нет данных в листе 'Критерии'")
        return
    
    # Работаем с БД
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            criteria_table = f"{portal_name}_criteria"
            groups_table = f"{portal_name}_criterion_groups"
            updated_data = []
            
            # Получаем карту: название группы -> id группы
            cursor.execute(f"SELECT id, name FROM {groups_table}")
            groups_map = {name: group_id for group_id, name in cursor.fetchall()}
            
            for row in sheet_data:
                criterion_id = str(row.get('id', '')).strip()
                criterion_name = str(row.get('name', '')).strip()
                group_name = str(row.get('group_id', '')).strip()  # В листе название группы
                prompt = str(row.get('prompt', '')).strip()
                show_text_description = row.get('show_text_description', True)
                evaluate_criterion = row.get('evaluate_criterion', True)
                include_in_score = row.get('include_in_score', True)
                include_in_entity_description = row.get('include_in_entity_description', False)
                llm_type = str(row.get('llm_type', 'standard')).strip()
                
                if not criterion_name or not group_name:
                    continue
                
                # Находим ID группы по названию
                group_id = groups_map.get(group_name)
                if not group_id:
                    print(f"Группа '{group_name}' не найдена в БД")
                    continue
                
                if criterion_id and criterion_id.isdigit():
                    # Обновляем существующий критерий по ID
                    cursor.execute(f"""
                        UPDATE {criteria_table} 
                        SET group_id = %s, name = %s, prompt = %s, 
                            show_text_description = %s, evaluate_criterion = %s,
                            include_in_score = %s, include_in_entity_description = %s,
                            llm_type = %s
                        WHERE id = %s
                        RETURNING id;
                    """, (group_id, criterion_name, prompt, show_text_description, 
                          evaluate_criterion, include_in_score, include_in_entity_description,
                          llm_type, int(criterion_id)))
                    
                    result = cursor.fetchone()
                    if result:
                        updated_data.append({
                            'id': str(result[0]),
                            'name': criterion_name,
                            'group_id': group_name,
                            'prompt': prompt,
                            'show_text_description': show_text_description,
                            'evaluate_criterion': evaluate_criterion,
                            'include_in_score': include_in_score,
                            'include_in_entity_description': include_in_entity_description,
                            'llm_type': llm_type
                        })
                        print(f"Обновлен критерий ID {criterion_id}: {criterion_name}")
                else:
                    # Проверяем, существует ли критерий с таким названием
                    cursor.execute(f"""
                        SELECT id FROM {criteria_table} WHERE name = %s
                    """, (criterion_name,))
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Обновляем существующий критерий по названию
                        existing_id = existing[0]
                        cursor.execute(f"""
                            UPDATE {criteria_table} 
                            SET group_id = %s, prompt = %s, 
                                show_text_description = %s, evaluate_criterion = %s,
                                include_in_score = %s, include_in_entity_description = %s,
                                llm_type = %s
                            WHERE id = %s
                            RETURNING id;
                        """, (group_id, prompt, show_text_description, 
                              evaluate_criterion, include_in_score, include_in_entity_description,
                              llm_type, existing_id))
                        
                        result = cursor.fetchone()
                        updated_data.append({
                            'id': str(result[0]),
                            'name': criterion_name,
                            'group_id': group_name,
                            'prompt': prompt,
                            'show_text_description': show_text_description,
                            'evaluate_criterion': evaluate_criterion,
                            'include_in_score': include_in_score,
                            'include_in_entity_description': include_in_entity_description,
                            'llm_type': llm_type
                        })
                        print(f"Обновлен критерий '{criterion_name}' (ID {existing_id}) - изменена группа на '{group_name}'")
                    else:
                        # Создаем новый критерий
                        cursor.execute(f"""
                            INSERT INTO {criteria_table} 
                                (group_id, name, prompt, show_text_description, evaluate_criterion,
                                 include_in_score, include_in_entity_description, llm_type) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                            RETURNING id;
                        """, (group_id, criterion_name, prompt, show_text_description,
                              evaluate_criterion, include_in_score, include_in_entity_description,
                              llm_type))
                        
                        new_id = cursor.fetchone()[0]
                        updated_data.append({
                            'id': str(new_id),
                            'name': criterion_name,
                            'group_id': group_name,
                            'prompt': prompt,
                            'show_text_description': show_text_description,
                            'evaluate_criterion': evaluate_criterion,
                            'include_in_score': include_in_score,
                            'include_in_entity_description': include_in_entity_description,
                            'llm_type': llm_type
                        })
                        print(f"Создан новый критерий ID {new_id}: {criterion_name}")
            
            conn.commit()
            
            # Обновляем Google Sheets с новыми ID (только колонку ID)
            if updated_data:
                try:
                    # Пытаемся обновить только колонку A (ID)
                    id_values = []
                    for i, row in enumerate(updated_data):
                        id_values.append([row['id']])
                    
                    # Обновляем колонку A, начиная со второй строки (пропуская заголовок)
                    worksheet.update(f'A2:A{len(updated_data)+1}', id_values, value_input_option='RAW')
                    print(f"Обновлены ID критериев в Google Sheets")
                except Exception as e:
                    print(f"Не удалось обновить Google Sheets: {e}")
                    print("ID критериев сохранены в БД, но не обновлены в таблице")
                
                print(f"Синхронизация критериев завершена для портала '{portal_name}'")


def sync_categories(portal_name, spreadsheet_id):
    """Синхронизирует категории из Google Sheets в БД"""
    client = get_google_sheets_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet('Категории')
    
    # Получаем все данные из листа
    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    
    # Преобразуем в список словарей
    categories_data = []
    for row in rows:
        if len(row) >= 4 and row[1].strip():  # Проверяем, что есть название
            categories_data.append({
                'id': str(row[0]).strip() if row[0] else '',
                'name': str(row[1]).strip(),
                'prompt': str(row[2]).strip(),
                'criteria': str(row[3]).strip()
            })
    
    categories_table = f"{portal_name}_categories"
    categories_criteria_table = f"{portal_name}_categories_criteria"
    criteria_table = f"{portal_name}_criteria"
    
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            updated_data = []
            
            for row_data in categories_data:
                category_id = row_data['id']
                category_name = row_data['name']
                prompt = row_data['prompt']
                criteria_names = row_data['criteria']
                
                if category_id and category_id.isdigit():
                    # Обновляем существующую категорию по ID
                    cursor.execute(f"""
                        UPDATE {categories_table} 
                        SET name = %s, prompt = %s
                        WHERE id = %s
                        RETURNING id;
                    """, (category_name, prompt, int(category_id)))
                    
                    result = cursor.fetchone()
                    if result:
                        db_category_id = result[0]
                        updated_data.append({
                            'id': str(db_category_id),
                            'name': category_name,
                            'prompt': prompt,
                            'criteria': criteria_names
                        })
                        print(f"Обновлена категория ID {category_id}: {category_name}")
                else:
                    # Проверяем, существует ли категория с таким названием
                    cursor.execute(f"""
                        SELECT id FROM {categories_table} WHERE name = %s
                    """, (category_name,))
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Обновляем существующую категорию по названию
                        existing_id = existing[0]
                        cursor.execute(f"""
                            UPDATE {categories_table} 
                            SET prompt = %s
                            WHERE id = %s
                            RETURNING id;
                        """, (prompt, existing_id))
                        
                        result = cursor.fetchone()
                        db_category_id = result[0]
                        updated_data.append({
                            'id': str(db_category_id),
                            'name': category_name,
                            'prompt': prompt,
                            'criteria': criteria_names
                        })
                        print(f"Обновлена категория '{category_name}' (ID {existing_id})")
                    else:
                        # Создаем новую категорию
                        cursor.execute(f"""
                            INSERT INTO {categories_table} (name, prompt) 
                            VALUES (%s, %s) 
                            RETURNING id;
                        """, (category_name, prompt))
                        
                        new_id = cursor.fetchone()[0]
                        updated_data.append({
                            'id': str(new_id),
                            'name': category_name,
                            'prompt': prompt,
                            'criteria': criteria_names
                        })
                        print(f"Создана новая категория ID {new_id}: {category_name}")
                
                # Синхронизируем связи с критериями
                if updated_data:
                    current_category = updated_data[-1]
                    sync_category_criteria(cursor, portal_name, int(current_category['id']), criteria_names)
            
            conn.commit()
            
            # Обновляем Google Sheets с новыми ID (только колонку ID)
            if updated_data:
                id_values = [[item['id']] for item in updated_data]
                try:
                    worksheet.update(f'A2:A{len(updated_data)+1}', id_values, value_input_option='RAW')
                    print(f"Обновлены ID категорий в Google Sheets")
                except Exception as e:
                    print(f"Ошибка при обновлении Google Sheets: {e}")


def sync_category_criteria(cursor, portal_name, category_id, criteria_names):
    """Синхронизирует связи между категорией и критериями"""
    if not criteria_names.strip():
        return
    
    categories_criteria_table = f"{portal_name}_categories_criteria"
    criteria_table = f"{portal_name}_criteria"
    
    # Удаляем старые связи для этой категории
    cursor.execute(f"""
        DELETE FROM {categories_criteria_table} WHERE category_id = %s
    """, (category_id,))
    
    # Разбираем список критериев
    criterion_names = [name.strip() for name in criteria_names.split(',') if name.strip()]
    
    # Получаем ID критериев по их названиям
    for criterion_name in criterion_names:
        cursor.execute(f"""
            SELECT id FROM {criteria_table} WHERE name = %s
        """, (criterion_name,))
        
        criterion_result = cursor.fetchone()
        if criterion_result:
            criterion_id = criterion_result[0]
            # Добавляем связь
            cursor.execute(f"""
                INSERT INTO {categories_criteria_table} (category_id, criterion_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (category_id, criterion_id))
            print(f"  Связана категория {category_id} с критерием '{criterion_name}' (ID {criterion_id})")
        else:
            print(f"  ВНИМАНИЕ: Критерий '{criterion_name}' не найден в БД")


def sync_all_portals():
    """Синхронизирует данные для всех порталов"""
    config = load_portals_config()
    portals = config.get('portals', [])
    
    for portal_config in portals:
        if isinstance(portal_config, str):
            continue
        
        portal_url = portal_config.get('url', '')
        spreadsheet_id = portal_config.get('googlespreadsheet_id', '')
        
        if not portal_url or not spreadsheet_id:
            continue
        
        portal_name = extract_portal_name(portal_url)
        if not portal_name:
            continue
        
        print(f"Обрабатываю портал: {portal_name}")
        
        try:
            sync_criterion_groups(portal_name, spreadsheet_id)
            sync_criteria(portal_name, spreadsheet_id)
            sync_categories(portal_name, spreadsheet_id)
        except Exception as e:
            print(f"Ошибка обработки портала {portal_name}: {e}")


def show_criteria_ids(portal_name):
    """Показывает ID критериев для копирования в Google Sheets"""
    with get_db_client() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT c.id, c.name, g.name as group_name 
                FROM {portal_name}_criteria c 
                JOIN {portal_name}_criterion_groups g ON c.group_id = g.id 
                ORDER BY c.id;
            """)
            criteria = cursor.fetchall()
            
            print(f"\nID критериев для портала {portal_name}:")
            print("ID\tНазвание\tГруппа")
            print("-" * 60)
            for criterion in criteria:
                print(f"{criterion[0]}\t{criterion[1]}\t{criterion[2]}")


if __name__ == "__main__":
    sync_all_portals()
    
    # Показываем ID для копирования в Google Sheets
    show_criteria_ids("advertpro")

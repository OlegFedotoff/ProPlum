import yaml
import re
from core.database import Database
from core.config import Config


def copy_object(db: Database, object_id=None, object_name=None, env_list=None):
    """
    Копирует объект из таблицы fw.objects в буфер обмена в формате YAML
    
    Args:
        db: объект Database
        object_id: ID объекта для копирования
        object_name: имя объекта для копирования (альтернатива object_id)
        env_list: список сред для получения данных (по умолчанию ['dev', 'prod'])
    """

    import pyperclip

    if env_list is None:
        env_list = ['dev', 'prod']
    
    if not object_id and not object_name:
        print("!!! Необходимо указать либо --id, либо --name")
        return False
    
    # Получаем данные объекта из разных сред
    env_data = {}
    base_object = None
    
    for env in env_list:
        print(f"Подключение к среде {env}...")
        
        # Получаем соединение для конкретной среды
        if env == 'dev':
            env_db = db.get_new_db(Config.db_dev) if hasattr(Config, 'db_dev') else db
        elif env == 'prod':
            env_db = db.get_new_db(Config.db_prod) if hasattr(Config, 'db_prod') else db
        else:
            env_db = db  # fallback для других сред
            
        if not env_db:
            print(f"!!! Не удалось подключиться к среде {env}")
            continue
            
        # Формируем SQL запрос
        if object_id:
            sql = "SELECT * FROM fw.objects WHERE object_id = %s"
            params = [object_id]
        else:
            sql = "SELECT * FROM fw.objects WHERE object_name = %s"
            params = [object_name]
            
        error_text, row = env_db.get_first_row(sql, params)
        
        if error_text:
            print(f"!!! Ошибка при получении данных из среды {env}: {error_text}")
            continue
            
        if not row:
            print(f"!!! Объект не найден в среде {env}")
            continue
            
        # Преобразуем строку в словарь
        columns = [
            'object_id', 'object_name', 'object_desc', 'extraction_type', 'load_type',
            'merge_key', 'delta_field', 'delta_field_format', 'delta_safety_period',
            'bdate_field', 'bdate_field_format', 'bdate_safety_period', 'load_method',
            'job_name', 'responsible_mail', 'priority', 'periodicity', 'load_interval',
            'activitystart', 'activityend', 'active', 'load_start_date', 'delta_start_date',
            'delta_mode', 'connect_string', 'load_function_name', 'where_clause',
            'load_group', 'src_date_type', 'src_ts_type', 'column_name_mapping',
            'transform_mapping', 'delta_field_type', 'bdate_field_type', 'param_list'
        ]
        
        def _format_interval_hhmmss(val):
            try:
                # timedelta
                if hasattr(val, 'total_seconds'):
                    total_seconds = int(val.total_seconds())
                else:
                    s = str(val) if val is not None else ''
                    # HH:MM:SS
                    m = re.match(r'^\s*(\d+):([0-5]\d):([0-5]\d)\s*$', s)
                    if m:
                        hours, minutes, seconds = map(int, m.groups())
                        total_seconds = hours*3600 + minutes*60 + seconds
                    else:
                        # D day(s), HH:MM:SS
                        m = re.match(r'^\s*(\d+)\s+day[s]?,\s*(\d+):([0-5]\d):([0-5]\d)\s*$', s)
                        if m:
                            days, hours, minutes, seconds = map(int, m.groups())
                            total_seconds = days*86400 + hours*3600 + minutes*60 + seconds
                        else:
                            # X hours | X minutes | X seconds
                            m = re.match(r'^\s*(\d+)\s*hour[s]?\s*$', s)
                            if m:
                                total_seconds = int(m.group(1)) * 3600
                            else:
                                m = re.match(r'^\s*(\d+)\s*minute[s]?\s*$', s)
                                if m:
                                    total_seconds = int(m.group(1)) * 60
                                else:
                                    m = re.match(r'^\s*(\d+)\s*second[s]?\s*$', s)
                                    if m:
                                        total_seconds = int(m.group(1))
                                    else:
                                        return s if s != '' else None
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            except Exception:
                return str(val) if val is not None else None

        obj_data = {}
        for i, col in enumerate(columns):
            value = row[i] if i < len(row) else None
            if value is not None:
                # Специальная обработка для некоторых типов
                if col in ['delta_safety_period', 'bdate_safety_period', 'periodicity', 'load_interval']:
                    obj_data[col] = _format_interval_hhmmss(value)
                elif isinstance(value, list):
                    # Обработка массивов
                    if col == 'responsible_mail' and len(value) == 1 and isinstance(value[0], str) and ';' in value[0]:
                        # Разбиваем строку с разделителями на отдельные элементы
                        obj_data[col] = [email.strip() for email in value[0].split(';') if email.strip()]
                    else:
                        obj_data[col] = value
                elif col in ['column_name_mapping', 'transform_mapping', 'param_list'] and value:
                    obj_data[col] = value  # JSON уже распарсен psycopg2
                elif col in ['activitystart', 'activityend'] and value:
                    obj_data[col] = str(value)
                elif col in ['load_start_date', 'delta_start_date'] and value:
                    obj_data[col] = value.strftime('%Y-%m-%d %H:%M:%S') if hasattr(value, 'strftime') else str(value)
                else:
                    obj_data[col] = value
            else:
                obj_data[col] = None
                
        env_data[env] = obj_data
        
        if base_object is None:
            base_object = obj_data.copy()
            
        print(f"Данные получены из среды {env}")
    
    if not env_data:
        print("!!! Не удалось получить данные ни из одной среды")
        return False
        
    # Создаем YAML структуру
    yaml_data = {
        'type': 'fw_object',
        'description': f"Объект {base_object.get('object_name', 'unknown')} - {base_object.get('object_desc', '')}",
        'params': {}
    }
    
    # Находим общие параметры для всех сред
    from collections import OrderedDict
    all_params = OrderedDict()
    env_specific_params = {env: OrderedDict() for env in env_data.keys()}
    
    # Получаем все возможные ключи
    all_keys = set()
    for env_obj in env_data.values():
        all_keys.update(env_obj.keys())
    
    # Строго соблюдаем порядок полей как в таблице
    for key in [
        'object_id', 'object_name', 'object_desc', 'extraction_type', 'load_type',
        'merge_key', 'delta_field', 'delta_field_format', 'delta_safety_period',
        'bdate_field', 'bdate_field_format', 'bdate_safety_period', 'load_method',
        'job_name', 'responsible_mail', 'priority', 'periodicity', 'load_interval',
        'activitystart', 'activityend', 'active', 'load_start_date', 'delta_start_date',
        'delta_mode', 'connect_string', 'load_function_name', 'where_clause',
        'load_group', 'src_date_type', 'src_ts_type', 'column_name_mapping',
        'transform_mapping', 'delta_field_type', 'bdate_field_type', 'param_list'
    ]:
        # Значения по средам
        values = {env: env_obj.get(key) for env, env_obj in env_data.items()}
        # Равны ли значения во всех средах
        def norm(v):
            if isinstance(v, dict):
                import json
                return json.dumps(v, sort_keys=True, ensure_ascii=False)
            return '' if v is None else str(v)
        unique_values = {norm(v) for v in values.values()}
        if len(unique_values) == 1:
            # Общее значение -> в all; даже если пустое, поле обязано быть выведено
            all_params[key] = list(values.values())[0]
        else:
            # В all кладем пустое значение, а различия — в соответствующие env
            all_params[key] = None
            for env, v in values.items():
                env_specific_params[env][key] = v
    
    # Формируем финальную структуру params
    if all_params:
        yaml_data['params']['all'] = all_params
    # Добавляем env-секции в алфавитном порядке, сохраняя порядок полей
    for env in sorted(env_specific_params.keys()):
        yaml_data['params'][env] = env_specific_params[env]
    
    # Конвертируем в YAML
    try:
        # Создаем безопасный дампер, который не будет сериализовать Python объекты
        from yaml import SafeDumper
        from yaml.representer import SafeRepresenter
        
        class CustomSafeDumper(SafeDumper):
            pass
        
        # Ensure OrderedDict is representable and preserves insertion order
        try:
            from collections import OrderedDict as _OrderedDict
            CustomSafeDumper.add_representer(_OrderedDict, SafeRepresenter.represent_dict)
        except Exception:
            pass
        
        yaml_output = yaml.dump(yaml_data, 
                               Dumper=CustomSafeDumper,
                               default_flow_style=False, 
                               allow_unicode=True, 
                               sort_keys=False,
                               indent=2)
        
        # Копируем в буфер обмена
        pyperclip.copy(yaml_output)
        
        print("✓ Данные объекта скопированы в буфер обмена")
        print(f"✓ Объект: {base_object.get('object_name', 'unknown')}")
        print(f"✓ Описание: {base_object.get('object_desc', '')}")
        print(f"✓ Обработано сред: {', '.join(env_data.keys())}")
        print(f"✓ Общих параметров: {len(all_params)}")
        print(f"✓ Специфичных параметров: {sum(len(params) for params in env_specific_params.values())}")
        
        return True
        
    except Exception as e:
        print(f"!!! Ошибка при создании YAML: {e}")
        return False

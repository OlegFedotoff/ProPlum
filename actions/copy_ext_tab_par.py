import yaml
from core.database import Database
from core.config import Config


def copy_ext_tables_params(db: Database, object_id=None, env_list=None):
    """
    Копирует запись из таблицы fw.ext_tables_params в буфер обмена в формате YAML
    
    Args:
        db: объект Database
        object_id: ID объекта для копирования
        env_list: список сред для получения данных (по умолчанию ['dev', 'prod'])
    """

    import pyperclip

    if env_list is None:
        env_list = ['dev', 'prod']
    
    if not object_id:
        print("!!! Необходимо указать --id")
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
        sql = """SELECT 
            object_id, load_method, connection_string, additional, active
        FROM fw.ext_tables_params WHERE object_id = %s"""
        params = [object_id]
            
        error_text, row = env_db.get_first_row(sql, params)
        
        if error_text:
            print(f"!!! Ошибка при получении данных из среды {env}: {error_text}")
            continue
            
        if not row:
            print(f"!!! Запись не найдена в среде {env}")
            continue
            
        # Преобразуем строку в словарь
        columns = [
            'object_id', 'load_method', 'connection_string', 'additional', 'active'
        ]
        
        obj_data = {}
        for i, col in enumerate(columns):
            value = row[i] if i < len(row) else None
            if value is not None:
                # Специальная обработка для некоторых типов
                if col == 'additional' and value:
                    obj_data[col] = value  # JSON уже распарсен psycopg2
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
        'type': 'fw_ext_tables_params',
        'description': f"Параметры внешних таблиц для object_id {base_object.get('object_id', 'unknown')} - метод {base_object.get('load_method', '')}",
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
    for key in ['object_id', 'load_method', 'connection_string', 'additional', 'active']:
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
        
        print("✓ Данные записи скопированы в буфер обмена")
        print(f"✓ Object ID: {base_object.get('object_id', 'unknown')}")
        print(f"✓ Load Method: {base_object.get('load_method', '')}")
        print(f"✓ Обработано сред: {', '.join(env_data.keys())}")
        print(f"✓ Общих параметров: {len(all_params)}")
        print(f"✓ Специфичных параметров: {sum(len(params) for params in env_specific_params.values())}")
        
        return True
        
    except Exception as e:
        print(f"!!! Ошибка при создании YAML: {e}")
        return False


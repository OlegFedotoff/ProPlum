# Миграция параметров внешних таблиц (fw.ext_tables_params)

## Обзор

Система поддерживает автоматизированную загрузку и управление параметрами внешних таблиц через YAML файлы. Это позволяет версионировать строки подключения для различных сред (dev, test, prod) и управлять ими через систему миграций.

## Архитектура

### 1. Таблицы базы данных

**fw.ext_tables_params** - основная таблица с параметрами:
- `object_id` - ID объекта из fw.objects
- `load_method` - метод загрузки (gpfdist, pxf и т.д.)
- `connection_string` - строка подключения
- `additional` - дополнительные параметры
- `active` - флаг активности

**fw.ext_tables_params_log** - история изменений:
- Все поля из основной таблицы
- `change_type` - тип изменения (INSERT/UPDATE)
- `change_timestamp` - время изменения
- `change_username` - пользователь

### 2. SQL функции

**fw.f_save_ext_tables_params_core(fw.ext_tables_params)** - основная функция UPSERT:
- Проверяет существование записи по `object_id` + `load_method`
- Выполняет INSERT для новых записей
- Выполняет UPDATE для существующих (с сохранением NULL полей через COALESCE)
- Возвращает результат и тип операции

**fw.f_save_ext_tables_params(fw.ext_tables_params)** - обертка с логированием:
- Вызывает core функцию
- Записывает историю в ext_tables_params_log
- Доступна для role_data_loader

### 3. Python обработчики

**core/handler.py**:
- `process_yaml_fw_ext_tables_params()` - парсинг YAML файлов
- `call_fw_save_ext_tables_params()` - формирование SQL и вызов функции

**actions/apply.py**:
- Поддержка типа `fw_ext_tables_params` при применении изменений

**actions/copy_ext_tab_par.py**:
- Копирование существующих параметров из БД в формате YAML

## Формат YAML файла

```yaml
type: fw_ext_tables_params
description: Описание параметров

params:
  # Общие параметры для всех сред
  all:
    object_id: 1234          # Обязательное поле
    load_method: gpfdist     # Обязательное поле
    active: true             # Необязательное (default: true)
  
  # Параметры специфичные для среды DEV
  dev:
    connection_string: 'gpfdist://dev-server:8081/data/*.csv'
    additional: 'FORMAT ''CSV'' DELIMITER '','''
  
  # Параметры специфичные для среды PROD
  prod:
    connection_string: 'gpfdist://prod-server:8081/data/*.csv'
    additional: 'FORMAT ''CSV'' DELIMITER '','''
```

### Правила объединения параметров:

1. Сначала берутся параметры из секции `all`
2. Затем они переопределяются параметрами из секции среды (`dev`, `test`, `prod`)
3. Поля со значением `NULL` в обновлении сохраняют старое значение (COALESCE)

## Использование

### 1. Копирование существующих параметров из БД

```bash
# Копирует параметры в буфер обмена в формате YAML
python manage.py copy_ext_tab_par -id <object_id> -envlist dev,prod
```

### 2. Создание нового YAML файла

Создайте файл с расширением `.yaml`:

```yaml
type: fw_ext_tables_params
description: Настройка подключения для объекта X

params:
  all:
    object_id: 5678
    load_method: pxf
    active: true
  dev:
    connection_string: 'pxf://dev-hdfs/path/to/data'
  prod:
    connection_string: 'pxf://prod-hdfs/path/to/data'
```

### 3. Применение через релиз

**a) Добавьте файл в директорию релиза:**
```
fw/releases/000024/
  ├── migration.json
  └── ext_params_update.yaml
```

**b) Зарегистрируйте в migration.json:**
```json
{
  "release": "000024",
  "migrations": [
    {
      "migration": "ext_params_update.yaml",
      "rollback": ""
    }
  ]
}
```

**c) Выполните миграцию:**
```bash
python manage.py migrate -s <set_name> -e dev
python manage.py migrate -s <set_name> -e prod
```

### 4. Применение через систему изменений

**a) Поместите файл в директорию changes:**
```
fw/changes/<set_name>/
  ├── migration.json
  ├── create.sql
  └── ext_params.yaml
```

**b) Зарегистрируйте в migration.json:**
```json
{
  "migrations": [
    {
      "migration": "ext_params.yaml",
      "rollback": ""
    }
  ]
}
```

**c) Примените изменение:**
```bash
python manage.py apply -s <set_name>
```

## Примеры использования

### Пример 1: Обновление connection_string для всех сред

```yaml
type: fw_ext_tables_params
description: Обновление хоста для gpfdist

params:
  all:
    object_id: 100
    load_method: gpfdist
  dev:
    connection_string: 'gpfdist://new-dev-host:8081/*.csv'
  prod:
    connection_string: 'gpfdist://new-prod-host:8081/*.csv'
```

### Пример 2: Добавление новых параметров

```yaml
type: fw_ext_tables_params
description: Настройка PXF для нового объекта

params:
  all:
    object_id: 200
    load_method: pxf
    active: true
  dev:
    connection_string: 'pxf://hdfs-dev/user/data/source'
    additional: 'PROFILE ''hdfs:text'''
  prod:
    connection_string: 'pxf://hdfs-prod/user/data/source'
    additional: 'PROFILE ''hdfs:text'''
```

### Пример 3: Деактивация параметров

```yaml
type: fw_ext_tables_params
description: Деактивация старого метода загрузки

params:
  all:
    object_id: 150
    load_method: gpfdist
    active: false
```

## Логика работы SQL функций

### INSERT (новая запись)
```sql
-- Если запись с (object_id, load_method) не существует
INSERT INTO fw.ext_tables_params (object_id, load_method, ...)
VALUES (1234, 'gpfdist', ...);

-- Записывается лог
INSERT INTO fw.ext_tables_params_log (...)
VALUES (..., 'INSERT', CURRENT_TIMESTAMP, session_user);
```

### UPDATE (существующая запись)
```sql
-- Если запись существует
UPDATE fw.ext_tables_params
SET connection_string = COALESCE(new_value, old_value),
    additional = COALESCE(new_value, old_value),
    active = COALESCE(new_value, old_value)
WHERE object_id = 1234 AND load_method = 'gpfdist';

-- Записывается лог
INSERT INTO fw.ext_tables_params_log (...)
VALUES (..., 'UPDATE', CURRENT_TIMESTAMP, session_user);
```

## Отличия от fw.objects

| Аспект | fw.objects | fw.ext_tables_params |
|--------|------------|---------------------|
| Первичный ключ | object_id | object_id + load_method |
| Количество полей | 35+ | 5 |
| Тип данных | Композитный type | Простая таблица |
| Логика UPSERT | По object_id | По object_id + load_method |
| Применение | Определение объектов загрузки | Настройка строк подключения |

## Лучшие практики

1. **Версионирование**: Всегда используйте систему миграций для изменений
2. **Среды**: Четко разделяйте параметры для dev/test/prod
3. **Описание**: Добавляйте понятные описания в YAML файлы
4. **Тестирование**: Сначала применяйте на dev, потом на prod
5. **Резервное копирование**: Используйте `copy_ext_tab_par` перед изменениями
6. **История**: Все изменения логируются в ext_tables_params_log

## Диагностика

### Проверка параметров в БД
```sql
-- Все параметры для объекта
SELECT * FROM fw.ext_tables_params WHERE object_id = 1234;

-- История изменений
SELECT * FROM fw.ext_tables_params_log 
WHERE object_id = 1234 
ORDER BY change_timestamp DESC;
```

### Отладка миграций
```bash
# Миграция с продолжением при ошибках
python manage.py migrate -s <set_name> -e dev --continue

# Просмотр логов миграций
python manage.py migrations -s <set_name> -e dev -l 2
```

## Создание файлов

Все необходимые файлы созданы:
- ✅ `fw/tables/ext_tables_params_log.sql` - таблица логов
- ✅ `fw/functions/f_save_ext_tables_params_core(fw.ext_tables_params).sql` - core функция
- ✅ `fw/functions/f_save_ext_tables_params(fw.ext_tables_params).sql` - wrapper функция
- ✅ `core/handler.py` - обработчики YAML
- ✅ `actions/apply.py` - поддержка в apply
- ✅ `actions/copy_ext_tab_par.py` - копирование из БД

## Следующие шаги

1. Создайте таблицу логов в БД:
   ```bash
   psql -f fw/tables/ext_tables_params_log.sql
   ```

2. Создайте функции:
   ```bash
   psql -f fw/functions/f_save_ext_tables_params_core\(fw.ext_tables_params\).sql
   psql -f fw/functions/f_save_ext_tables_params\(fw.ext_tables_params\).sql
   ```

3. Протестируйте копирование:
   ```bash
   python manage.py copy_ext_tab_par -id <existing_object_id>
   ```

4. Создайте тестовый YAML файл и примените его через apply или migrate


# Быстрый старт: fw_ext_tables_params

## 🚀 Установка (5 минут)

### 1. Установите SQL объекты
```bash
psql -d your_database -f install_ext_tables_params_support.sql
```

### 2. Проверьте работу команды копирования
```bash
# Найдите существующий object_id в таблице
python manage.py copy_ext_tab_par -id <existing_object_id> -envlist dev,prod
```

YAML скопируется в буфер обмена! ✅

## 📝 Быстрое создание YAML

### Минимальный пример
```yaml
type: fw_ext_tables_params
description: Мои параметры

params:
  all:
    object_id: 1234
    load_method: gpfdist
    active: true
  dev:
    connection_string: 'gpfdist://dev-host:8081/data/*.csv'
  prod:
    connection_string: 'gpfdist://prod-host:8081/data/*.csv'
```

Сохраните как `my_params.yaml`

## ⚡ Применение

### Вариант 1: Через изменения (для разработки)
```bash
# 1. Поместите YAML в fw/changes/<set_name>/
cp my_params.yaml fw/changes/my_set/

# 2. Зарегистрируйте в migration.json
# Добавьте: {"migration": "my_params.yaml", "rollback": ""}

# 3. Примените
python manage.py apply -s my_set
```

### Вариант 2: Через релиз (для prod)
```bash
# 1. Поместите в релиз
cp my_params.yaml fw/releases/000025/

# 2. Зарегистрируйте в migration.json релиза

# 3. Мигрируйте
python manage.py migrate -s my_set -e dev
python manage.py migrate -s my_set -e prod
```

## 🔍 Проверка результата

```sql
-- Проверить данные
SELECT * FROM fw.ext_tables_params WHERE object_id = 1234;

-- Проверить историю
SELECT * FROM fw.ext_tables_params_log 
WHERE object_id = 1234 
ORDER BY change_timestamp DESC;
```

## 💡 Типичные сценарии

### Сценарий 1: Обновить connection_string
```yaml
type: fw_ext_tables_params
description: Обновление хоста

params:
  all:
    object_id: 100
    load_method: gpfdist
  dev:
    connection_string: 'gpfdist://new-host-dev:8081/*.csv'
  prod:
    connection_string: 'gpfdist://new-host-prod:8081/*.csv'
```

### Сценарий 2: Добавить новый метод загрузки
```yaml
type: fw_ext_tables_params
description: Добавление PXF

params:
  all:
    object_id: 100
    load_method: pxf  # Новый метод!
    active: true
  dev:
    connection_string: 'pxf://hdfs-dev/path'
  prod:
    connection_string: 'pxf://hdfs-prod/path'
```

### Сценарий 3: Деактивировать параметры
```yaml
type: fw_ext_tables_params
description: Отключение старого метода

params:
  all:
    object_id: 100
    load_method: gpfdist
    active: false  # Деактивируем
```

## 🎯 Команды шпаргалка

```bash
# Копировать из БД
python manage.py copy_ext_tab_par -id <id> -envlist dev,prod

# Применить изменение (dev)
python manage.py apply -s <set>

# Мигрировать релиз (dev)
python manage.py migrate -s <set> -e dev

# Мигрировать релиз (prod)
python manage.py migrate -s <set> -e prod

# Просмотр логов миграций
python manage.py migrations -s <set> -e dev -l 2
```

## ⚠️ Важные моменты

1. **Обязательные поля**: `object_id`, `load_method`
2. **Уникальность**: Комбинация `object_id` + `load_method`
3. **NULL поля**: При UPDATE сохраняют старое значение
4. **Секции**: `all` применяется первой, затем `dev`/`prod`
5. **История**: Все изменения логируются автоматически

## 🆘 Устранение проблем

### Ошибка: "object_id is mandatory"
✅ Убедитесь, что в секции `all` есть `object_id`

### Ошибка: "load_method is mandatory"
✅ Убедитесь, что в секции `all` есть `load_method`

### Данные не обновляются
✅ Проверьте:
1. Правильно ли указана среда (`-e dev/prod`)
2. Есть ли в YAML секция для этой среды
3. Посмотрите лог: `fw.ext_tables_params_log`

### YAML не применяется
✅ Проверьте:
1. Расширение файла `.yaml`
2. Тип в файле: `type: fw_ext_tables_params`
3. Зарегистрирован ли в `migration.json`

## 📚 Полная документация

- **MIGRATION_EXT_TABLES_PARAMS.md** - Полное руководство
- **SUMMARY_EXT_TABLES_PARAMS.md** - Техническое резюме
- **fw/changes/.example_ext_tables_params.yaml** - Пример файла

## ✨ Готово!

Вы готовы использовать `fw_ext_tables_params` через YAML миграции! 🎉


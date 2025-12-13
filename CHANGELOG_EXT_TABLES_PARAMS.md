# Changelog: Поддержка fw_ext_tables_params через YAML

## Дата: 2025-11-21
## Автор: Oleg Fedotov
## Ветка: fw_Oleg_Fedotov

---

## 📋 Описание изменений

Реализована полная поддержка управления параметрами внешних таблиц (`fw.ext_tables_params`) через YAML файлы в системе миграций. Теперь можно версионировать и автоматически применять строки подключения для разных сред аналогично работе с `fw.objects`.

---

## 🆕 Новые файлы

### SQL объекты (3 файла)

1. **fw/tables/ext_tables_params_log.sql**
   - Таблица для логирования истории изменений
   - Поля: log_id, object_id, load_method, connection_string, additional, active, change_type, change_timestamp, change_username
   - Distributed: REPLICATED

2. **fw/functions/f_save_ext_tables_params_core(fw.ext_tables_params).sql**
   - Основная функция UPSERT с SECURITY DEFINER
   - Выполняет INSERT для новых записей
   - Выполняет UPDATE для существующих с COALESCE
   - Ключ: object_id + load_method (композитный)

3. **fw/functions/f_save_ext_tables_params(fw.ext_tables_params).sql**
   - Wrapper функция без SECURITY DEFINER
   - Вызывает core функцию
   - Записывает историю в ext_tables_params_log
   - GRANT для role_data_loader

### Python модули (1 файл)

4. **actions/copy_ext_tab_par.py**
   - Команда для копирования параметров из БД в формат YAML
   - Поддержка множественных сред (dev, prod, test)
   - Автоматическое разделение на общие и специфичные параметры
   - Копирование в буфер обмена через pyperclip

### Документация (4 файла)

5. **MIGRATION_EXT_TABLES_PARAMS.md**
   - Полная техническая документация
   - Описание архитектуры и компонентов
   - Примеры использования
   - Лучшие практики

6. **SUMMARY_EXT_TABLES_PARAMS.md**
   - Краткое резюме реализации
   - Чек-лист готовности
   - Сравнение с fw.objects

7. **QUICK_START_EXT_TABLES_PARAMS.md**
   - Быстрый старт за 5 минут
   - Шпаргалка по командам
   - Типичные сценарии
   - Устранение проблем

8. **install_ext_tables_params_support.sql**
   - Скрипт автоматической установки
   - Создает все SQL объекты
   - Проверяет корректность установки

### Примеры (1 файл)

9. **fw/changes/.example_ext_tables_params.yaml**
   - Пример YAML файла
   - Комментарии по использованию
   - Шаблон для копирования

---

## ✏️ Измененные файлы

### Python модули (3 файла)

1. **actions/__init__.py**
   - ➕ Добавлен импорт: `from .copy_ext_tab_par import *`
   - Исправлена ошибка: `AttributeError: module 'actions' has no attribute 'copy_ext_tables_params'`

2. **core/handler.py** (2 новых метода)
   - ➕ `process_yaml_fw_ext_tables_params(yaml_data, env)` (строки 410-467)
     - Парсинг YAML файлов типа `fw_ext_tables_params`
     - Объединение параметров из секций `all` и `env`
     - Логирование выполнения
   
   - ➕ `call_fw_save_ext_tables_params(params)` (строки 468-503)
     - Формирование SQL вызова с ROW конструктором
     - Обработка типов данных (bool, int, string)
     - Вызов функции `fw.f_save_ext_tables_params()`
   
   - ➕ Добавлена обработка в `migrate_file()` (строка 697)
     - Поддержка типа `fw_ext_tables_params` в YAML файлах релизов

3. **actions/apply.py**
   - ➕ Добавлена обработка типа `fw_ext_tables_params` (строки 199-203)
   - Делегирование обработки в `handler.process_yaml_fw_ext_tables_params()`

### Управляющие файлы (1 файл)

4. **manage.py**
   - ➕ Добавлен action: `copy_ext_tab_par` в choices (строка 109)
   - ➕ Добавлена обработка команды (строки 89-90)
   - Параметры: `-id`, `-envlist`

---

## 🔧 Архитектура решения

```
┌─────────────────────────────────────────────────────────────┐
│                      YAML файл                              │
│              type: fw_ext_tables_params                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│            Python: handler.py                               │
│   process_yaml_fw_ext_tables_params()                       │
│   - Парсинг YAML                                            │
│   - Объединение all + env параметров                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│            Python: handler.py                               │
│   call_fw_save_ext_tables_params()                          │
│   - Формирование ROW конструктора                           │
│   - SQL: SELECT fw.f_save_ext_tables_params(ROW(...))       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│         PostgreSQL: f_save_ext_tables_params()              │
│   - Wrapper без SECURITY DEFINER                            │
│   - Вызов core функции                                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│      PostgreSQL: f_save_ext_tables_params_core()            │
│   - SECURITY DEFINER                                        │
│   - UPSERT по (object_id, load_method)                      │
│   - COALESCE для NULL значений                              │
└────────┬─────────────────────────────────────┬──────────────┘
         │                                     │
         ▼                                     ▼
┌──────────────────────┐          ┌──────────────────────────┐
│ fw.ext_tables_params │          │ fw.ext_tables_params_log │
│   INSERT/UPDATE      │          │     INSERT (история)     │
└──────────────────────┘          └──────────────────────────┘
```

---

## 🎯 Основные возможности

1. **Версионирование параметров подключения**
   - Хранение в Git вместе с кодом
   - История всех изменений

2. **Автоматическое применение**
   - Через систему миграций (releases)
   - Через систему изменений (changes)

3. **Многосредовость**
   - Разные параметры для dev/test/prod
   - Общие параметры в секции `all`

4. **Полное логирование**
   - Автоматическая запись в ext_tables_params_log
   - Тип изменения (INSERT/UPDATE)
   - Временные метки и пользователи

5. **UPSERT логика**
   - INSERT при отсутствии записи
   - UPDATE с сохранением NULL полей через COALESCE

6. **Копирование из БД**
   - Команда `copy_ext_tab_par`
   - Вывод в формате YAML
   - Готово к использованию

---

## 📝 Примеры использования

### 1. Копирование существующих параметров
```bash
python manage.py copy_ext_tab_par -id 1234 -envlist dev,prod
# Результат в буфере обмена, готов к вставке
```

### 2. Создание YAML файла
```yaml
type: fw_ext_tables_params
description: Обновление хоста gpfdist

params:
  all:
    object_id: 1234
    load_method: gpfdist
    active: true
  dev:
    connection_string: 'gpfdist://dev-server:8081/data/*.csv'
  prod:
    connection_string: 'gpfdist://prod-server:8081/data/*.csv'
```

### 3. Применение через изменения
```bash
# Поместить в fw/changes/<set_name>/params.yaml
python manage.py apply -s <set_name>
```

### 4. Применение через релиз
```bash
# Поместить в fw/releases/000025/params.yaml
python manage.py migrate -s <set_name> -e dev
python manage.py migrate -s <set_name> -e prod
```

---

## ✅ Тестирование

### Единичное тестирование
```sql
-- 1. Вставка новой записи
SELECT fw.f_save_ext_tables_params(
    ROW(1234, 'gpfdist', 'gpfdist://host:8081/*.csv', 'FORMAT CSV', true)::fw.ext_tables_params
);

-- 2. Проверка вставки
SELECT * FROM fw.ext_tables_params WHERE object_id = 1234;

-- 3. Проверка лога
SELECT * FROM fw.ext_tables_params_log WHERE object_id = 1234;

-- 4. Обновление записи
SELECT fw.f_save_ext_tables_params(
    ROW(1234, 'gpfdist', 'gpfdist://new-host:8081/*.csv', NULL, NULL)::fw.ext_tables_params
);

-- 5. Проверка COALESCE (additional и active не изменились)
SELECT * FROM fw.ext_tables_params WHERE object_id = 1234;
```

### Интеграционное тестирование
```bash
# 1. Копирование
python manage.py copy_ext_tab_par -id 100

# 2. Создание YAML
# Вставить из буфера в файл test.yaml

# 3. Применение
python manage.py apply -s test_set

# 4. Проверка в БД
# SELECT * FROM fw.ext_tables_params WHERE object_id = 100;
```

---

## 🔒 Безопасность

1. **SECURITY DEFINER** - Core функция выполняется с правами владельца
2. **Раздельные права** - Wrapper доступен через GRANT
3. **Логирование** - Все изменения записываются с session_user
4. **Аудит** - Полная история в ext_tables_params_log

---

## 📊 Статистика

- **Новых файлов**: 9
- **Измененных файлов**: 4
- **Строк кода SQL**: ~150
- **Строк кода Python**: ~200
- **Строк документации**: ~800

---

## 🚀 Установка

### Быстрая установка
```bash
# В PostgreSQL/Greenplum
psql -d your_database -f install_ext_tables_params_support.sql
```

### Ручная установка
```bash
psql -d your_database -f fw/tables/ext_tables_params_log.sql
psql -d your_database -f fw/functions/f_save_ext_tables_params_core\(fw.ext_tables_params\).sql
psql -d your_database -f fw/functions/f_save_ext_tables_params\(fw.ext_tables_params\).sql
```

---

## 📚 Документация

| Файл | Назначение |
|------|-----------|
| MIGRATION_EXT_TABLES_PARAMS.md | Полная техническая документация |
| SUMMARY_EXT_TABLES_PARAMS.md | Краткое резюме |
| QUICK_START_EXT_TABLES_PARAMS.md | Быстрый старт |
| CHANGELOG_EXT_TABLES_PARAMS.md | Этот файл - описание изменений |

---

## ⚠️ Совместимость

- **PostgreSQL**: 9.4+
- **Greenplum**: 5.0+
- **Python**: 3.6+
- **Зависимости**: pyperclip, pyyaml

---

## 🔄 Обратная совместимость

✅ Все изменения обратно совместимы:
- Существующие миграции не затронуты
- Команда `copy_ext_tab_par` - новая
- Тип `fw_ext_tables_params` - новый
- Таблица `ext_tables_params` не изменена

---

## 🎯 TODO (будущие улучшения)

- [ ] Добавить rollback поддержку для YAML
- [ ] UI для редактирования параметров
- [ ] Валидация connection_string
- [ ] Поддержка bulk операций
- [ ] Экспорт всех параметров объекта

---

## 👤 Контакты

- **Автор**: Oleg Fedotov
- **Дата**: 21.11.2025
- **Ветка**: fw_Oleg_Fedotov

---

## ✨ Итог

Система ProPlum теперь полностью поддерживает управление параметрами внешних таблиц через YAML миграции. Все параметры версионируются, логируются и могут быть применены автоматически для разных сред.

**Статус**: ✅ Готово к использованию


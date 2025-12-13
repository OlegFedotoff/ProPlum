-- Установка поддержки fw_ext_tables_params
-- Этот скрипт создает необходимые объекты для работы с параметрами внешних таблиц через YAML миграции

\echo '===== Установка поддержки fw_ext_tables_params ====='
\echo ''

-- 1. Создание таблицы логов
\echo '1. Создание таблицы fw.ext_tables_params_log...'
\i fw/tables/ext_tables_params_log.sql
\echo 'OK'
\echo ''

-- 2. Создание core функции
\echo '2. Создание функции fw.f_save_ext_tables_params_core...'
\i fw/functions/f_save_ext_tables_params_core(fw.ext_tables_params).sql
\echo 'OK'
\echo ''

-- 3. Создание wrapper функции
\echo '3. Создание функции fw.f_save_ext_tables_params...'
\i fw/functions/f_save_ext_tables_params(fw.ext_tables_params).sql
\echo 'OK'
\echo ''

-- 4. Проверка созданных объектов
\echo '4. Проверка созданных объектов:'
\echo ''

\echo 'Таблица ext_tables_params_log:'
SELECT count(*) as column_count 
FROM information_schema.columns 
WHERE table_schema = 'fw' 
  AND table_name = 'ext_tables_params_log';

\echo ''
\echo 'Функции:'
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'fw'
  AND routine_name LIKE '%save_ext_tables_params%'
ORDER BY routine_name;

\echo ''
\echo '===== Установка завершена успешно! ====='
\echo ''
\echo 'Следующие шаги:'
\echo '1. Используйте: python manage.py copy_ext_tab_par -id <object_id>'
\echo '2. Создайте YAML файл с типом: fw_ext_tables_params'
\echo '3. Примените через: python manage.py apply или migrate'
\echo ''


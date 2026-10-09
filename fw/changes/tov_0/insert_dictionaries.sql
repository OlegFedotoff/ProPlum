-- Добавление справочных значений для новой архитектуры загрузки
-- (см. dags/dags_it/dags_load_crm/docs/TARGET_ARCHITECTURE.md, раздел 15.1)

-- load_method = zservice_raw: ingestion по асинхронному ZService
-- с прямой загрузкой через COPY в заранее созданную постоянную RAW-таблицу
INSERT INTO fw.d_load_method (load_method, desc_short)
SELECT 'zservice_raw', 'Загрузка ZService в RAW-таблицу'
WHERE NOT EXISTS (SELECT 1 FROM fw.d_load_method WHERE load_method = 'zservice_raw');

-- load_type = dbt_queue: тип загрузки объектов новой архитектуры,
-- очереди dbt-партиций (transformation handler не выбирает)
INSERT INTO fw.d_load_type (load_type, desc_short)
SELECT 'dbt_queue', 'Очередь dbt'
WHERE NOT EXISTS (SELECT 1 FROM fw.d_load_type WHERE load_type = 'dbt_queue');
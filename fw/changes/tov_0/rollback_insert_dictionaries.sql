-- Откат справочных значений новой архитектуры загрузки

DELETE FROM fw.d_load_method WHERE load_method = 'zservice_raw';

DELETE FROM fw.d_load_type WHERE load_type = 'dbt_queue';
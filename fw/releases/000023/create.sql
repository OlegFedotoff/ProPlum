all: |

    INSERT INTO fw.d_load_method (load_method, desc_short, desc_middle, desc_long) 
    VALUES('zservice_async', 'Загрузка через zservice в асинхронном режиме', 'Load from zservice async', NULL);

prod: |

    DROP TABLE fw.ext_tables_params;-- cascade;

    CREATE FOREIGN TABLE fw.ext_tables_params (
    object_id int8 NULL, 
    load_method text NULL, 
    connection_string text NULL, 
    additional text NULL, 
    active bool NULL
    )
    SERVER pg_fw_prod
    OPTIONS (table_name 'ext_tables_params');

    COMMENT ON TABLE fw.ext_tables_params IS 'Расширенные параметры строки подключения для внешних таблиц (для метода загрузки gpfdist)';

    -- Column comments

    COMMENT ON COLUMN fw.ext_tables_params.object_id IS 'id объекта из objects';
    COMMENT ON COLUMN fw.ext_tables_params.load_method IS 'Метод загрузки (код)';
    COMMENT ON COLUMN fw.ext_tables_params.connection_string IS 'Строка подключения';
    COMMENT ON COLUMN fw.ext_tables_params.additional IS 'Дополнительная информация к строке подключения';
    COMMENT ON COLUMN fw.ext_tables_params.active IS 'Флаг активности записи';


dev: |
  
    DROP TABLE fw.ext_tables_params;-- cascade;

    CREATE FOREIGN TABLE fw.ext_tables_params (
    object_id int8 NULL, 
    load_method text NULL, 
    connection_string text NULL, 
    additional text NULL, 
    active bool NULL
    )
    SERVER pg_fw_dev
    OPTIONS (table_name 'ext_tables_params');

    COMMENT ON TABLE fw.ext_tables_params IS 'Расширенные параметры строки подключения для внешних таблиц (для метода загрузки gpfdist)';

    -- Column comments

    COMMENT ON COLUMN fw.ext_tables_params.object_id IS 'id объекта из objects';
    COMMENT ON COLUMN fw.ext_tables_params.load_method IS 'Метод загрузки (код)';
    COMMENT ON COLUMN fw.ext_tables_params.connection_string IS 'Строка подключения';
    COMMENT ON COLUMN fw.ext_tables_params.additional IS 'Дополнительная информация к строке подключения';
    COMMENT ON COLUMN fw.ext_tables_params.active IS 'Флаг активности записи';
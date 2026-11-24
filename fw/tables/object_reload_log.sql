all: |
    DROP FOREIGN TABLE IF EXISTS fw.object_reload_log;
dev: |
    CREATE FOREIGN TABLE fw.object_reload_log (
        object_id int8 NOT NULL,
        load_id int8 NULL,
        reload_from date NULL,
        reload_to date NULL,
        valid_to date NULL,
        requested_by text NULL,
        requested_dttm timestamp NULL,
        change_type text NULL,
        change_timestamp timestamp NULL,
        change_username text NULL
    )
    SERVER pg_fw_dev
    OPTIONS (table_name 'object_reload_log');

    COMMENT ON FOREIGN TABLE fw.object_reload_log IS 'История изменений fw.object_reload - заполняется правилами object_reload_insert/update/delete в PostgreSQL.';
    COMMENT ON COLUMN fw.object_reload_log.object_id IS 'Объект из fw.objects.';
    COMMENT ON COLUMN fw.object_reload_log.load_id IS 'load_id перезагрузки в fw.load_info (job_name = RELOAD).';
    COMMENT ON COLUMN fw.object_reload_log.reload_from IS 'Начало окна перезагрузки, включительно.';
    COMMENT ON COLUMN fw.object_reload_log.reload_to IS 'Конец окна перезагрузки, не включительно.';
    COMMENT ON COLUMN fw.object_reload_log.valid_to IS 'Последний день действия запроса.';
    COMMENT ON COLUMN fw.object_reload_log.requested_by IS 'Пользователь GP, запросивший перезагрузку.';
    COMMENT ON COLUMN fw.object_reload_log.requested_dttm IS 'Момент создания запроса.';
    COMMENT ON COLUMN fw.object_reload_log.change_type IS 'Тип изменения: insert (запрос создан), update или delete (запрос завершён или отменён).';
    COMMENT ON COLUMN fw.object_reload_log.change_timestamp IS 'Момент изменения.';
    COMMENT ON COLUMN fw.object_reload_log.change_username IS 'Пользователь PostgreSQL, выполнивший изменение - при изменениях из GP это учётка user mapping.';
prod: |
    CREATE FOREIGN TABLE fw.object_reload_log (
        object_id int8 NOT NULL,
        load_id int8 NULL,
        reload_from date NULL,
        reload_to date NULL,
        valid_to date NULL,
        requested_by text NULL,
        requested_dttm timestamp NULL,
        change_type text NULL,
        change_timestamp timestamp NULL,
        change_username text NULL
    )
    SERVER pg_fw_prod
    OPTIONS (table_name 'object_reload_log');

    COMMENT ON FOREIGN TABLE fw.object_reload_log IS 'История изменений fw.object_reload - заполняется правилами object_reload_insert/update/delete в PostgreSQL.';
    COMMENT ON COLUMN fw.object_reload_log.object_id IS 'Объект из fw.objects.';
    COMMENT ON COLUMN fw.object_reload_log.load_id IS 'load_id перезагрузки в fw.load_info (job_name = RELOAD).';
    COMMENT ON COLUMN fw.object_reload_log.reload_from IS 'Начало окна перезагрузки, включительно.';
    COMMENT ON COLUMN fw.object_reload_log.reload_to IS 'Конец окна перезагрузки, не включительно.';
    COMMENT ON COLUMN fw.object_reload_log.valid_to IS 'Последний день действия запроса.';
    COMMENT ON COLUMN fw.object_reload_log.requested_by IS 'Пользователь GP, запросивший перезагрузку.';
    COMMENT ON COLUMN fw.object_reload_log.requested_dttm IS 'Момент создания запроса.';
    COMMENT ON COLUMN fw.object_reload_log.change_type IS 'Тип изменения: insert (запрос создан), update или delete (запрос завершён или отменён).';
    COMMENT ON COLUMN fw.object_reload_log.change_timestamp IS 'Момент изменения.';
    COMMENT ON COLUMN fw.object_reload_log.change_username IS 'Пользователь PostgreSQL, выполнивший изменение - при изменениях из GP это учётка user mapping.';

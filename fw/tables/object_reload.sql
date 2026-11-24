all: |
    DROP FOREIGN TABLE IF EXISTS fw.object_reload;
dev: |
    CREATE FOREIGN TABLE fw.object_reload (
        object_id int8 NOT NULL,
        load_id int8 NOT NULL,
        reload_from date NOT NULL,
        reload_to date NOT NULL,
        valid_to date NOT NULL,
        requested_by text NULL,
        requested_dttm timestamp DEFAULT now() NOT NULL
    )
    SERVER pg_fw_dev
    OPTIONS (table_name 'object_reload');

    COMMENT ON FOREIGN TABLE fw.object_reload IS 'Активные запросы на перезагрузку dbt PARTITION объектов за период - по ним генератор DAG строит партиции перезагрузки вместо обычных.';
    COMMENT ON COLUMN fw.object_reload.object_id IS 'Объект из fw.objects - на объект допускается только один активный запрос.';
    COMMENT ON COLUMN fw.object_reload.load_id IS 'load_id перезагрузки в fw.load_info (job_name = RELOAD) - по нему запрос удаляется при завершении загрузки.';
    COMMENT ON COLUMN fw.object_reload.reload_from IS 'Начало окна перезагрузки, включительно.';
    COMMENT ON COLUMN fw.object_reload.reload_to IS 'Конец окна перезагрузки, не включительно.';
    COMMENT ON COLUMN fw.object_reload.valid_to IS 'Последний день действия запроса: генератор DAG учитывает запрос, только пока current_date <= valid_to.';
    COMMENT ON COLUMN fw.object_reload.requested_by IS 'Пользователь GP, запросивший перезагрузку (session_user в fw.f_request_reload).';
    COMMENT ON COLUMN fw.object_reload.requested_dttm IS 'Момент создания запроса.';
prod: |
    CREATE FOREIGN TABLE fw.object_reload (
        object_id int8 NOT NULL,
        load_id int8 NOT NULL,
        reload_from date NOT NULL,
        reload_to date NOT NULL,
        valid_to date NOT NULL,
        requested_by text NULL,
        requested_dttm timestamp DEFAULT now() NOT NULL
    )
    SERVER pg_fw_prod
    OPTIONS (table_name 'object_reload');

    COMMENT ON FOREIGN TABLE fw.object_reload IS 'Активные запросы на перезагрузку dbt PARTITION объектов за период - по ним генератор DAG строит партиции перезагрузки вместо обычных.';
    COMMENT ON COLUMN fw.object_reload.object_id IS 'Объект из fw.objects - на объект допускается только один активный запрос.';
    COMMENT ON COLUMN fw.object_reload.load_id IS 'load_id перезагрузки в fw.load_info (job_name = RELOAD) -  по нему запрос удаляется при завершении загрузки.';
    COMMENT ON COLUMN fw.object_reload.reload_from IS 'Начало окна перезагрузки, включительно.';
    COMMENT ON COLUMN fw.object_reload.reload_to IS 'Конец окна перезагрузки, не включительно.';
    COMMENT ON COLUMN fw.object_reload.valid_to IS 'Последний день действия запроса: генератор DAG учитывает запрос, только пока current_date <= valid_to.';
    COMMENT ON COLUMN fw.object_reload.requested_by IS 'Пользователь GP, запросивший перезагрузку (session_user в fw.f_request_reload).';
    COMMENT ON COLUMN fw.object_reload.requested_dttm IS 'Момент создания запроса.';

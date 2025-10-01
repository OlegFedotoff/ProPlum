-- fw.load_results определение

-- Drop table

-- DROP TABLE fw.load_results;

CREATE TABLE fw.load_results (
	result_id int8 NOT NULL, -- Идентификатор записи результата
	load_id int8 NOT NULL, -- Идентификатор загрузки (fw.load_info.load_id)
	object_id int8 NOT NULL, -- Идентификатор объекта (fw.objects.object_id)
	table_name text NOT NULL, -- Имя целевой таблицы (схема.таблица)
	rows_affected int8 DEFAULT 0 NOT NULL, -- Число строк, загруженных/измененных
	duration_ms int8 NULL, -- Длительность загрузки в миллисекундах
	started_at timestamp NULL, -- Время начала
	finished_at timestamp NULL, -- Время окончания
	username text DEFAULT "current_user"() NOT NULL, -- Кто сделал
	status text DEFAULT 'SUCCESS' NOT NULL, -- Статус результата: SUCCESS/ERROR
	error_message text NULL, -- Сообщение об ошибке при неуспехе
	extra text NULL, -- Дополнительные поля/метаданные операции
	CONSTRAINT pk_load_results PRIMARY KEY (result_id)
)
DISTRIBUTED BY (result_id);
COMMENT ON TABLE fw.load_results IS 'Результаты загрузок по таблицам (факт выполнения операций)';

-- Column comments

COMMENT ON COLUMN fw.load_results.result_id IS 'Идентификатор записи результата';
COMMENT ON COLUMN fw.load_results.load_id IS 'Идентификатор загрузки (fw.load_info.load_id)';
COMMENT ON COLUMN fw.load_results.object_id IS 'Идентификатор объекта (fw.objects.object_id)';
COMMENT ON COLUMN fw.load_results.table_name IS 'Имя целевой таблицы (схема.таблица)';
COMMENT ON COLUMN fw.load_results.rows_affected IS 'Число строк, загруженных/измененных';
COMMENT ON COLUMN fw.load_results.duration_ms IS 'Длительность загрузки в миллисекундах';
COMMENT ON COLUMN fw.load_results.started_at IS 'Время начала';
COMMENT ON COLUMN fw.load_results.finished_at IS 'Время окончания';
COMMENT ON COLUMN fw.load_results.username IS 'Кто сделал';
COMMENT ON COLUMN fw.load_results.status IS 'Статус результата: SUCCESS/ERROR';
COMMENT ON COLUMN fw.load_results.error_message IS 'Сообщение об ошибке при неуспехе';
COMMENT ON COLUMN fw.load_results.extra IS 'Дополнительные поля/метаданные операции';


GRANT ALL ON TABLE fw.load_results TO role_data_loader;
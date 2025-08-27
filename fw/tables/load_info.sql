-- fw.load_info определение

-- Drop table

-- DROP TABLE fw.load_info;

CREATE TABLE fw.load_info (
	load_id int8 NOT NULL, -- id загрузки, генерируется автоматически
	load_status int4 NOT NULL, -- Статус загрузки: 1 - новая, 2 - в процессе, 3 - успешно завершена, -1 - завершена с ошибкой
	object_id int8 NOT NULL, -- id объекта из objects
	extraction_type text NULL, -- Способ экстракции данных из таблицы-источника: DELTA, PARTITION, FULL
	load_type text NULL, -- Тип загрузки целевой таблицы: DELTA, DELTA_MERGE, DELTA_PARTITION, DELTA_UPSERT, FULL
	extraction_from timestamp NULL, -- Нижняя граница интервала экстракции
	extraction_to timestamp NULL, -- Верхняя граница интервала экстракции
	load_from timestamp NULL, -- Нижняя граница интервала загрузки
	load_to timestamp NULL, -- Верхняя граница интервала загрузки
	load_method text NULL, -- Метод загрузки - способ попадания данных в стейджинг: pxf, gpfdist, dblink, python
	job_name text NULL, -- Имя задания Airflow
	created_dttm timestamp DEFAULT now() NOT NULL, -- Метка времени создания load_id
	updated_dttm timestamp DEFAULT now() NOT NULL, -- Метка времени изменения load_id
	row_cnt int8 NULL, -- Количество записей, измененных в процессе загрузки
	CONSTRAINT fk_extraction_type FOREIGN KEY (extraction_type) REFERENCES fw.d_extraction_type(extraction_type),
	CONSTRAINT fk_load_method FOREIGN KEY (load_method) REFERENCES fw.d_load_method(load_method),
	CONSTRAINT fk_load_status FOREIGN KEY (load_status) REFERENCES fw.d_load_status(load_status),
	CONSTRAINT fk_load_type FOREIGN KEY (load_type) REFERENCES fw.d_load_type(load_type)
)
DISTRIBUTED BY (load_id);
COMMENT ON TABLE fw.load_info IS 'Информация по заданиям загрузки данных и их статус';

-- Column comments

COMMENT ON COLUMN fw.load_info.load_id IS 'id загрузки, генерируется автоматически';
COMMENT ON COLUMN fw.load_info.load_status IS 'Статус загрузки: 1 - новая, 2 - в процессе, 3 - успешно завершена, -1 - завершена с ошибкой';
COMMENT ON COLUMN fw.load_info.object_id IS 'id объекта из objects';
COMMENT ON COLUMN fw.load_info.extraction_type IS 'Способ экстракции данных из таблицы-источника: DELTA, PARTITION, FULL';
COMMENT ON COLUMN fw.load_info.load_type IS 'Тип загрузки целевой таблицы: DELTA, DELTA_MERGE, DELTA_PARTITION, DELTA_UPSERT, FULL';
COMMENT ON COLUMN fw.load_info.extraction_from IS 'Нижняя граница интервала экстракции';
COMMENT ON COLUMN fw.load_info.extraction_to IS 'Верхняя граница интервала экстракции';
COMMENT ON COLUMN fw.load_info.load_from IS 'Нижняя граница интервала загрузки';
COMMENT ON COLUMN fw.load_info.load_to IS 'Верхняя граница интервала загрузки';
COMMENT ON COLUMN fw.load_info.load_method IS 'Метод загрузки - способ попадания данных в стейджинг: pxf, gpfdist, dblink, python';
COMMENT ON COLUMN fw.load_info.job_name IS 'Имя задания Airflow';
COMMENT ON COLUMN fw.load_info.created_dttm IS 'Метка времени создания load_id';
COMMENT ON COLUMN fw.load_info.updated_dttm IS 'Метка времени изменения load_id';
COMMENT ON COLUMN fw.load_info.row_cnt IS 'Количество записей, измененных в процессе загрузки';
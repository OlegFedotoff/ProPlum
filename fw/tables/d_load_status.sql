-- fw.d_load_status определение

-- Drop table

-- DROP TABLE fw.d_load_status;

CREATE TABLE fw.d_load_status (
	load_status int4 NOT NULL, -- Статус загрузки (код)
	desc_short text NULL, -- Статус загрузки (короткое описание)
	desc_middle text NULL, -- Статус загрузки (среднее описание)
	desc_long text NULL, -- Статус загрузки (длинное описание)
	CONSTRAINT pk_load_status PRIMARY KEY (load_status)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.d_load_status IS 'Справочник значений поля таблицы load_info.load_status, содержит описание статусов загрузки данных для соответствующего load_id';

-- Column comments

COMMENT ON COLUMN fw.d_load_status.load_status IS 'Статус загрузки (код)';
COMMENT ON COLUMN fw.d_load_status.desc_short IS 'Статус загрузки (короткое описание)';
COMMENT ON COLUMN fw.d_load_status.desc_middle IS 'Статус загрузки (среднее описание)';
COMMENT ON COLUMN fw.d_load_status.desc_long IS 'Статус загрузки (длинное описание)';



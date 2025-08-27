-- fw.d_load_group определение

-- Drop table

-- DROP TABLE fw.d_load_group;

CREATE TABLE fw.d_load_group (
	load_group text NOT NULL, -- Группа загрузок (код)
	desc_short text NULL, -- Группа загрузок (короткое описание)
	desc_middle text NULL, -- Группа загрузок (среднее описание)
	desc_long text NULL, -- Группа загрузок (длинное описание)
	CONSTRAINT pk_load_group PRIMARY KEY (load_group)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.d_load_group IS 'Справочник значений поля таблицы objects.load_group, содержит описание групп загрузки в Airflow';

-- Column comments

COMMENT ON COLUMN fw.d_load_group.load_group IS 'Группа загрузок (код)';
COMMENT ON COLUMN fw.d_load_group.desc_short IS 'Группа загрузок (короткое описание)';
COMMENT ON COLUMN fw.d_load_group.desc_middle IS 'Группа загрузок (среднее описание)';
COMMENT ON COLUMN fw.d_load_group.desc_long IS 'Группа загрузок (длинное описание)';



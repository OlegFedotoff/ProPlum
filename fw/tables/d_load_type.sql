-- fw.d_load_type определение

-- Drop table

-- DROP TABLE fw.d_load_type;

CREATE TABLE fw.d_load_type (
	load_type text NOT NULL, -- Тип загрузки (код)
	desc_short text NULL, -- Тип загрузки (короткое описание)
	desc_middle text NULL, -- Тип загрузки (среднее описание)
	desc_long text NULL, -- Тип загрузки (длинное описание)
	CONSTRAINT pk_load_type PRIMARY KEY (load_type)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.d_load_type IS 'Справочник значений поля таблицы objects.load_type, содержит описание типов загрузки данных в целевую таблицу';

-- Column comments

COMMENT ON COLUMN fw.d_load_type.load_type IS 'Тип загрузки (код)';
COMMENT ON COLUMN fw.d_load_type.desc_short IS 'Тип загрузки (короткое описание)';
COMMENT ON COLUMN fw.d_load_type.desc_middle IS 'Тип загрузки (среднее описание)';
COMMENT ON COLUMN fw.d_load_type.desc_long IS 'Тип загрузки (длинное описание)';



-- fw.d_extraction_type определение

-- Drop table

-- DROP TABLE fw.d_extraction_type;

CREATE TABLE fw.d_extraction_type (
	extraction_type text NOT NULL, -- Режим экстракции (код)
	desc_short text NULL, -- Режим экстракции (короткое описание)
	desc_middle text NULL, -- Режим экстракции (среднее описание)
	desc_long text NULL, -- Режим экстракции (длинное описание)
	CONSTRAINT pk_extraction_type PRIMARY KEY (extraction_type)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.d_extraction_type IS 'Справочник значений поля таблицы objects.extraction_type, содержит описание режимов экстракции данных из источника';

-- Column comments

COMMENT ON COLUMN fw.d_extraction_type.extraction_type IS 'Режим экстракции (код)';
COMMENT ON COLUMN fw.d_extraction_type.desc_short IS 'Режим экстракции (короткое описание)';
COMMENT ON COLUMN fw.d_extraction_type.desc_middle IS 'Режим экстракции (среднее описание)';
COMMENT ON COLUMN fw.d_extraction_type.desc_long IS 'Режим экстракции (длинное описание)';



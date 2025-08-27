-- fw.d_delta_mode определение

-- Drop table

-- DROP TABLE fw.d_delta_mode;

CREATE TABLE fw.d_delta_mode (
	delta_mode text NOT NULL, -- Режим дельты (код)
	desc_short text NULL, -- Режим дельты (короткое описание)
	desc_middle text NULL, -- Режим дельты (среднее описание)
	desc_long text NULL, -- Режим дельты (длинное описание)
	CONSTRAINT pk_delta_mode PRIMARY KEY (delta_mode)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.d_delta_mode IS 'Справочник значений поля таблицы objects.delta_mode, содержит описание режимов дельты';

-- Column comments

COMMENT ON COLUMN fw.d_delta_mode.delta_mode IS 'Режим дельты (код)';
COMMENT ON COLUMN fw.d_delta_mode.desc_short IS 'Режим дельты (короткое описание)';
COMMENT ON COLUMN fw.d_delta_mode.desc_middle IS 'Режим дельты (среднее описание)';
COMMENT ON COLUMN fw.d_delta_mode.desc_long IS 'Режим дельты (длинное описание)';



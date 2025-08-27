-- fw.d_load_method определение

-- Drop table

-- DROP TABLE fw.d_load_method;

CREATE TABLE fw.d_load_method (
	load_method text NOT NULL, -- Метод загрузки (код)
	desc_short text NULL, -- Метод загрузки (короткое описание)
	desc_middle text NULL, -- Метод загрузки (среднее описание)
	desc_long text NULL, -- Метод загрузки (длинное описание)
	CONSTRAINT pk_load_method PRIMARY KEY (load_method)
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.d_load_method IS 'Справочник значений поля таблицы objects.load_method, содержит описание методов загрузки или расчета данных';

-- Column comments

COMMENT ON COLUMN fw.d_load_method.load_method IS 'Метод загрузки (код)';
COMMENT ON COLUMN fw.d_load_method.desc_short IS 'Метод загрузки (короткое описание)';
COMMENT ON COLUMN fw.d_load_method.desc_middle IS 'Метод загрузки (среднее описание)';
COMMENT ON COLUMN fw.d_load_method.desc_long IS 'Метод загрузки (длинное описание)';



-- fw.load_constants определение

-- Drop table

-- DROP TABLE fw.load_constants;

CREATE TABLE fw.load_constants (
	constant_name text NULL, -- Имя константы
	constant_type text NULL, -- Тип возвращаемого значения для константы
	constant_value text NULL, -- Значение константы
	load_type text NULL
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.load_constants IS 'Константы для настройки процесса загрузки данных';

-- Column comments

COMMENT ON COLUMN fw.load_constants.constant_name IS 'Имя константы';
COMMENT ON COLUMN fw.load_constants.constant_type IS 'Тип возвращаемого значения для константы';
COMMENT ON COLUMN fw.load_constants.constant_value IS 'Значение константы';



-- fw.ext_tables_params определение

-- Drop table

-- DROP TABLE fw.ext_tables_params;

CREATE TABLE fw.ext_tables_params (
	object_id int8 NULL, -- id объекта из objects
	load_method text NULL, -- Метод загрузки (код)
	connection_string text NULL, -- Строка подключения
	additional text NULL, -- Дополнительная информация к строке подключения
	"active" bool NULL -- Флаг активности записи
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.ext_tables_params IS 'Расширенные параметры строки подключения для внешних таблиц (для метода загрузки gpfdist)';

-- Column comments

COMMENT ON COLUMN fw.ext_tables_params.object_id IS 'id объекта из objects';
COMMENT ON COLUMN fw.ext_tables_params.load_method IS 'Метод загрузки (код)';
COMMENT ON COLUMN fw.ext_tables_params.connection_string IS 'Строка подключения';
COMMENT ON COLUMN fw.ext_tables_params.additional IS 'Дополнительная информация к строке подключения';
COMMENT ON COLUMN fw.ext_tables_params."active" IS 'Флаг активности записи';



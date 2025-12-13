-- fw.ext_tables_params_log определение
-- История изменений параметров внешних таблиц

-- Drop table

-- DROP TABLE fw.ext_tables_params_log;

CREATE TABLE fw.ext_tables_params_log (
	log_id bigserial NOT NULL,
	object_id int8 NULL, -- id объекта из objects
	load_method text NULL, -- Метод загрузки (код)
	connection_string text NULL, -- Строка подключения
	additional text NULL, -- Дополнительная информация к строке подключения
	"active" bool NULL, -- Флаг активности записи
	change_type text NULL, -- Тип изменения: INSERT, UPDATE, DELETE
	change_timestamp timestamp NULL DEFAULT CURRENT_TIMESTAMP, -- Время изменения
	change_username text NULL DEFAULT session_user -- Пользователь, внесший изменение
)
DISTRIBUTED REPLICATED;

COMMENT ON TABLE fw.ext_tables_params_log IS 'История изменений расширенных параметров строки подключения для внешних таблиц';

-- Column comments

COMMENT ON COLUMN fw.ext_tables_params_log.log_id IS 'ID записи лога';
COMMENT ON COLUMN fw.ext_tables_params_log.object_id IS 'id объекта из objects';
COMMENT ON COLUMN fw.ext_tables_params_log.load_method IS 'Метод загрузки (код)';
COMMENT ON COLUMN fw.ext_tables_params_log.connection_string IS 'Строка подключения';
COMMENT ON COLUMN fw.ext_tables_params_log.additional IS 'Дополнительная информация к строке подключения';
COMMENT ON COLUMN fw.ext_tables_params_log."active" IS 'Флаг активности записи';
COMMENT ON COLUMN fw.ext_tables_params_log.change_type IS 'Тип изменения: INSERT, UPDATE, DELETE';
COMMENT ON COLUMN fw.ext_tables_params_log.change_timestamp IS 'Время изменения';
COMMENT ON COLUMN fw.ext_tables_params_log.change_username IS 'Пользователь, внесший изменение';


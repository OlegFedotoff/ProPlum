-- fw.logs определение

-- Drop table

-- DROP TABLE fw.logs;

CREATE TABLE fw.logs (
	log_id int8 NOT NULL, -- id лога, генерируется автоматически
	load_id int8 NULL, -- id загрузки, заполняется при загрузке с помощью фреймворка
	log_timestamp timestamp DEFAULT now() NOT NULL, -- Метка времени записи лога
	log_type text NOT NULL, -- Тип лога: ERROR, INFO, WARN, DEBUG
	log_msg text NOT NULL, -- Содержимое лога
	log_location text NULL, -- Место возникновения сообщения
	is_error bool NULL, -- Метка ошибки - если тип лога - ERROR
	log_user text DEFAULT "current_user"() NULL,
	CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);
COMMENT ON TABLE fw.logs IS 'Логи выполнения операций фреймворка при загрузке данных';

-- Column comments

COMMENT ON COLUMN fw.logs.log_id IS 'id лога, генерируется автоматически';
COMMENT ON COLUMN fw.logs.load_id IS 'id загрузки, заполняется при загрузке с помощью фреймворка';
COMMENT ON COLUMN fw.logs.log_timestamp IS 'Метка времени записи лога';
COMMENT ON COLUMN fw.logs.log_type IS 'Тип лога: ERROR, INFO, WARN, DEBUG';
COMMENT ON COLUMN fw.logs.log_msg IS 'Содержимое лога';
COMMENT ON COLUMN fw.logs.log_location IS 'Место возникновения сообщения';
COMMENT ON COLUMN fw.logs.is_error IS 'Метка ошибки - если тип лога - ERROR';



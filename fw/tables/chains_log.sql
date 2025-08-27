-- fw.chains_log определение

-- Drop table

-- DROP TABLE fw.chains_log;

CREATE TABLE fw.chains_log (
	log_id int8 NOT NULL, -- ID сообщения лога
	instance_id int8 NULL, -- id запуска цепочки
	log_timestamp timestamp NULL, -- Метка времени возникновения сообщения
	log_type text NULL, -- Тип сообщения
	log_msg text NULL, -- Сообщение
	CONSTRAINT pk_chain_log_id PRIMARY KEY (log_id),
	CONSTRAINT fk_instance_id FOREIGN KEY (instance_id) REFERENCES fw.chains_info(instance_id)
)
DISTRIBUTED BY (log_id);
COMMENT ON TABLE fw.chains_log IS 'Подробный лог выполнения цепочки';

-- Column comments

COMMENT ON COLUMN fw.chains_log.log_id IS 'ID сообщения лога';
COMMENT ON COLUMN fw.chains_log.instance_id IS 'id запуска цепочки';
COMMENT ON COLUMN fw.chains_log.log_timestamp IS 'Метка времени возникновения сообщения';
COMMENT ON COLUMN fw.chains_log.log_type IS 'Тип сообщения';
COMMENT ON COLUMN fw.chains_log.log_msg IS 'Сообщение';



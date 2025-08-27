-- fw.chains_info определение

-- Drop table

-- DROP TABLE fw.chains_info;

CREATE TABLE fw.chains_info (
	instance_id int8 NOT NULL, -- id запуска цепочки
	chain_name text NULL, -- Тех.имя цепочки процессов
	load_from timestamp NULL, -- Параметр генерации load_id для перегрузки данных (extraction_from)
	load_to timestamp NULL, -- Параметр генерации load_id для перегрузки данных (extraction_to)
	status int4 NULL, -- Статус загрузки цепочки
	chain_start timestamp NULL, -- Метка времени начала работы цепочки
	chain_finish timestamp NULL, -- Метка времени окончания работы цепочки
	CONSTRAINT pk_instance_id PRIMARY KEY (instance_id),
	CONSTRAINT fk_load_status FOREIGN KEY (status) REFERENCES fw.d_load_status(load_status)
)
DISTRIBUTED BY (instance_id);
COMMENT ON TABLE fw.chains_info IS 'Информация о запусках цепочек процессов';

-- Column comments

COMMENT ON COLUMN fw.chains_info.instance_id IS 'id запуска цепочки';
COMMENT ON COLUMN fw.chains_info.chain_name IS 'Тех.имя цепочки процессов';
COMMENT ON COLUMN fw.chains_info.load_from IS 'Параметр генерации load_id для перегрузки данных (extraction_from)';
COMMENT ON COLUMN fw.chains_info.load_to IS 'Параметр генерации load_id для перегрузки данных (extraction_to)';
COMMENT ON COLUMN fw.chains_info.status IS 'Статус загрузки цепочки';
COMMENT ON COLUMN fw.chains_info.chain_start IS 'Метка времени начала работы цепочки';
COMMENT ON COLUMN fw.chains_info.chain_finish IS 'Метка времени окончания работы цепочки';



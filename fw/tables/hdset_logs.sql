-- fw.hdset_logs определение

-- Drop table

-- DROP TABLE fw.hdset_logs;

CREATE TABLE fw.hdset_logs (
	log_id int8 NOT NULL, -- Идентификатор
	release_id int8 NULL, -- Миграция
	migration_id int8 NULL, -- Миграция
	i_time timestamp NULL, -- Время 
	i_user varchar(255) NULL, -- Кто выполнил действие
	status varchar(20) NULL, -- Статус: S,E
	action_type varchar(20) NULL, -- Тип действия: M,R,O
	object_type varchar(255) NULL, -- Тип объекта
	object_schema varchar(255) NULL, -- Схема
	object_name varchar(255) NULL, -- Имя объекта
	action_info varchar(2000) NULL, -- Описание действия
	error_text varchar(2000) NULL, -- Ошибка
	CONSTRAINT hdset_logs_pkey PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);
CREATE INDEX i_hdset_logs_migration ON fw.hdset_logs USING btree (migration_id);
CREATE INDEX i_hdset_logs_release ON fw.hdset_logs USING btree (release_id);
COMMENT ON TABLE fw.hdset_logs IS 'Логирование исполнения файлов';

-- Column comments

COMMENT ON COLUMN fw.hdset_logs.log_id IS 'Идентификатор';
COMMENT ON COLUMN fw.hdset_logs.release_id IS 'Миграция';
COMMENT ON COLUMN fw.hdset_logs.migration_id IS 'Миграция';
COMMENT ON COLUMN fw.hdset_logs.i_time IS 'Время ';
COMMENT ON COLUMN fw.hdset_logs.i_user IS 'Кто выполнил действие';
COMMENT ON COLUMN fw.hdset_logs.status IS 'Статус: S,E';
COMMENT ON COLUMN fw.hdset_logs.action_type IS 'Тип действия: M,R,O';
COMMENT ON COLUMN fw.hdset_logs.object_type IS 'Тип объекта';
COMMENT ON COLUMN fw.hdset_logs.object_schema IS 'Схема';
COMMENT ON COLUMN fw.hdset_logs.object_name IS 'Имя объекта';
COMMENT ON COLUMN fw.hdset_logs.action_info IS 'Описание действия';
COMMENT ON COLUMN fw.hdset_logs.error_text IS 'Ошибка';



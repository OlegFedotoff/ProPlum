-- fw.hdset_migrations определение

-- Drop table

-- DROP TABLE fw.hdset_migrations;

CREATE TABLE fw.hdset_migrations (
	migration_id int8 NOT NULL, -- Идентификатор
	release_id int8 NOT NULL, -- Релиз
	migration_file varchar(255) NULL, -- Название файла миграции
	rollback_file varchar(255) NULL, -- Название файла отката
	success_cnt int8 NULL, -- Число успешно выполненных запросов
	error_cnt int8 NULL, -- Число ошибочных запросов
	status varchar(20) NULL, -- Статус: I,S,E,R
	i_time timestamp NULL, -- Дата и время установки
	error_text varchar(2000) NULL, -- Текст сообщения с ошибкой
	migration_data text NULL, -- Содержимое файла с миграцией
	rollback_data text NULL, -- Содержимое файла отката
	CONSTRAINT hdset_migrations_pkey PRIMARY KEY (migration_id)
)
DISTRIBUTED BY (migration_id);
CREATE INDEX i_hdset_migrations_release ON fw.hdset_migrations USING btree (release_id);
COMMENT ON TABLE fw.hdset_migrations IS 'Список миграций в рамках релиза';

-- Column comments

COMMENT ON COLUMN fw.hdset_migrations.migration_id IS 'Идентификатор';
COMMENT ON COLUMN fw.hdset_migrations.release_id IS 'Релиз';
COMMENT ON COLUMN fw.hdset_migrations.migration_file IS 'Название файла миграции';
COMMENT ON COLUMN fw.hdset_migrations.rollback_file IS 'Название файла отката';
COMMENT ON COLUMN fw.hdset_migrations.success_cnt IS 'Число успешно выполненных запросов';
COMMENT ON COLUMN fw.hdset_migrations.error_cnt IS 'Число ошибочных запросов';
COMMENT ON COLUMN fw.hdset_migrations.status IS 'Статус: I,S,E,R';
COMMENT ON COLUMN fw.hdset_migrations.i_time IS 'Дата и время установки';
COMMENT ON COLUMN fw.hdset_migrations.error_text IS 'Текст сообщения с ошибкой';
COMMENT ON COLUMN fw.hdset_migrations.migration_data IS 'Содержимое файла с миграцией';
COMMENT ON COLUMN fw.hdset_migrations.rollback_data IS 'Содержимое файла отката';



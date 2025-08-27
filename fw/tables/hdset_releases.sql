-- fw.hdset_releases определение

-- Drop table

-- DROP TABLE fw.hdset_releases;

CREATE TABLE fw.hdset_releases (
	release_id int8 NOT NULL, -- Идентификатор
	release_code varchar(255) NOT NULL, -- Код
	release_name varchar(255) NULL, -- Название
	release_comment text NULL, -- Описание
	file_cnt int8 NULL, -- Число файлов
	success_cnt int8 NULL, -- Число успешно выполненных запросов
	error_cnt int8 NULL, -- Число ошибочных запросов
	status varchar(20) NULL, -- Статус: I,S,E,D,R
	i_time timestamp NULL, -- Даьа и время установки релиза
	i_user varchar(255) NULL, -- Кто установил
	err_text text NULL, -- Текст ошибки
	CONSTRAINT hdset_releases_pkey PRIMARY KEY (release_id)
)
DISTRIBUTED BY (release_id);
COMMENT ON TABLE fw.hdset_releases IS 'Описания релизов';

-- Column comments

COMMENT ON COLUMN fw.hdset_releases.release_id IS 'Идентификатор';
COMMENT ON COLUMN fw.hdset_releases.release_code IS 'Код';
COMMENT ON COLUMN fw.hdset_releases.release_name IS 'Название';
COMMENT ON COLUMN fw.hdset_releases.release_comment IS 'Описание';
COMMENT ON COLUMN fw.hdset_releases.file_cnt IS 'Число файлов';
COMMENT ON COLUMN fw.hdset_releases.success_cnt IS 'Число успешно выполненных запросов';
COMMENT ON COLUMN fw.hdset_releases.error_cnt IS 'Число ошибочных запросов';
COMMENT ON COLUMN fw.hdset_releases.status IS 'Статус: I,S,E,D,R';
COMMENT ON COLUMN fw.hdset_releases.i_time IS 'Даьа и время установки релиза';
COMMENT ON COLUMN fw.hdset_releases.i_user IS 'Кто установил';
COMMENT ON COLUMN fw.hdset_releases.err_text IS 'Текст ошибки';



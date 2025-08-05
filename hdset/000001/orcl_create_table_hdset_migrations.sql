
-------------------------------------------------------------------------
-- Table name: hdset_migrations
-- Список миграций в рамках релиза
-------------------------------------------------------------------------
CREATE TABLE hdset_migrations(
  migration_id        NUMBER(16),
  release_id          NUMBER(16) NOT NULL,
  migration_file      VARCHAR2(255),
  rollback_file       VARCHAR2(255),
  success_cnt         NUMBER(16),
  error_cnt           NUMBER(16),
  status              VARCHAR2(20),
  i_time              DATE,
  error_text          VARCHAR2(2000),
  migration_data      CLOB,
  rollback_data       CLOB
)
  TABLESPACE sandbox
;

ALTER TABLE hdset_migrations
  ADD CONSTRAINT pk_hdset_migrations PRIMARY KEY(migration_id) USING INDEX TABLESPACE sandbox;

CREATE INDEX i_hdset_migrations_release ON hdset_migrations(release_id) TABLESPACE sandbox;

COMMENT ON TABLE hdset_migrations                           IS 'Список миграций в рамках релиза';
COMMENT ON COLUMN hdset_migrations.migration_id             IS 'Идентификатор';
COMMENT ON COLUMN hdset_migrations.release_id               IS 'Релиз';
COMMENT ON COLUMN hdset_migrations.migration_file           IS 'Название файла миграции';
COMMENT ON COLUMN hdset_migrations.rollback_file            IS 'Название файла отката';
COMMENT ON COLUMN hdset_migrations.success_cnt              IS 'Число успешно выполненных запросов';
COMMENT ON COLUMN hdset_migrations.error_cnt                IS 'Число ошибочных запросов';
COMMENT ON COLUMN hdset_migrations.status                   IS 'Статус: I,S,E,R';
COMMENT ON COLUMN hdset_migrations.i_time                   IS 'Дата и время установки';
COMMENT ON COLUMN hdset_migrations.error_text               IS 'Текст сообщения с ошибкой';
COMMENT ON COLUMN hdset_migrations.migration_data           IS 'Содержимое файла с миграцией';
COMMENT ON COLUMN hdset_migrations.rollback_data            IS 'Содержимое файла отката';



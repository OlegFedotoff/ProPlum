
-------------------------------------------------------------------------
-- Table name: hdset_migrations
-- Список миграций в рамках релиза
-------------------------------------------------------------------------
CREATE TABLE hdset_migrations(
  migration_id        BIGINT PRIMARY KEY,
  release_id          BIGINT NOT NULL,
  migration_file      VARCHAR(255),
  rollback_file       VARCHAR(255),
  success_cnt         BIGINT,
  error_cnt           BIGINT,
  status              VARCHAR(20),
  i_time              TIMESTAMP,
  error_text          VARCHAR(2000),
  migration_data      TEXT,
  rollback_data       TEXT
)
;


CREATE INDEX i_hdset_migrations_release ON hdset_migrations(release_id);

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



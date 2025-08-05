

-------------------------------------------------------------------------
-- Table name: hdset_logs
-- Логирование исполнения файловв
-------------------------------------------------------------------------
CREATE TABLE hdset_logs(
  log_id        BIGINT PRIMARY KEY,
  release_id    BIGINT,
  migration_id  BIGINT,
  i_time        TIMESTAMP,
  i_user        VARCHAR(255),
  status        VARCHAR(20),
  action_type   VARCHAR(20),
  object_type   VARCHAR(255),
  object_schema VARCHAR(255),
  object_name   VARCHAR(255),
  action_info   VARCHAR(2000),
  error_text    VARCHAR(2000)
)
;

CREATE INDEX i_hdset_logs_migration ON hdset_logs(migration_id);
CREATE INDEX i_hdset_logs_release ON hdset_logs(release_id);

COMMENT ON TABLE hdset_logs                           IS 'Логирование исполнения файлов';
COMMENT ON COLUMN hdset_logs.log_id                   IS 'Идентификатор';
COMMENT ON COLUMN hdset_logs.release_id               IS 'Миграция';
COMMENT ON COLUMN hdset_logs.migration_id             IS 'Миграция';
COMMENT ON COLUMN hdset_logs.i_time                   IS 'Время ';
COMMENT ON COLUMN hdset_logs.i_user                   IS 'Кто выполнил действие';
COMMENT ON COLUMN hdset_logs.status                   IS 'Статус: S,E';
COMMENT ON COLUMN hdset_logs.action_type              IS 'Тип действия: M,R,O';
COMMENT ON COLUMN hdset_logs.object_type              IS 'Тип объекта';
COMMENT ON COLUMN hdset_logs.object_schema            IS 'Схема';
COMMENT ON COLUMN hdset_logs.object_name              IS 'Имя объекта';
COMMENT ON COLUMN hdset_logs.action_info              IS 'Описание действия';
COMMENT ON COLUMN hdset_logs.error_text               IS 'Ошибка';

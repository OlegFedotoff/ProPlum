

-------------------------------------------------------------------------
-- Table name: hdset_releases
-- Описания релизов
-------------------------------------------------------------------------
CREATE TABLE hdset_releases(
  release_id        BIGINT PRIMARY KEY,
  release_code      VARCHAR(255) NOT NULL,
  release_name      VARCHAR(255),
  release_comment   TEXT,
  file_cnt          BIGINT,
  success_cnt       BIGINT,
  error_cnt         BIGINT,
  status            VARCHAR(20),
  i_time            TIMESTAMP,
  i_user            VARCHAR(255),
  err_text          TEXT
)
;

COMMENT ON TABLE hdset_releases                           IS 'Описания релизов';
COMMENT ON COLUMN hdset_releases.release_id               IS 'Идентификатор';
COMMENT ON COLUMN hdset_releases.release_code             IS 'Код';
COMMENT ON COLUMN hdset_releases.release_name             IS 'Название';
COMMENT ON COLUMN hdset_releases.release_comment          IS 'Описание';
COMMENT ON COLUMN hdset_releases.file_cnt                 IS 'Число файлов';
COMMENT ON COLUMN hdset_releases.success_cnt              IS 'Число успешно выполненных запросов';
COMMENT ON COLUMN hdset_releases.error_cnt                IS 'Число ошибочных запросов';
COMMENT ON COLUMN hdset_releases.status                   IS 'Статус: I,S,E,D,R';
COMMENT ON COLUMN hdset_releases.i_time                   IS 'Даьа и время установки релиза';
COMMENT ON COLUMN hdset_releases.i_user                   IS 'Кто установил';
COMMENT ON COLUMN hdset_releases.err_text                 IS 'Текст ошибки';

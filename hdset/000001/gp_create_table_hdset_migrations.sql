
-------------------------------------------------------------------------
-- Table name: hdset_migrations
-- ������ �������� � ������ ������
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

COMMENT ON TABLE hdset_migrations                           IS '������ �������� � ������ ������';
COMMENT ON COLUMN hdset_migrations.migration_id             IS '�������������';
COMMENT ON COLUMN hdset_migrations.release_id               IS '�����';
COMMENT ON COLUMN hdset_migrations.migration_file           IS '�������� ����� ��������';
COMMENT ON COLUMN hdset_migrations.rollback_file            IS '�������� ����� ������';
COMMENT ON COLUMN hdset_migrations.success_cnt              IS '����� ������� ����������� ��������';
COMMENT ON COLUMN hdset_migrations.error_cnt                IS '����� ��������� ��������';
COMMENT ON COLUMN hdset_migrations.status                   IS '������: I,S,E,R';
COMMENT ON COLUMN hdset_migrations.i_time                   IS '���� � ����� ���������';
COMMENT ON COLUMN hdset_migrations.error_text               IS '����� ��������� � �������';
COMMENT ON COLUMN hdset_migrations.migration_data           IS '���������� ����� � ���������';
COMMENT ON COLUMN hdset_migrations.rollback_data            IS '���������� ����� ������';





-------------------------------------------------------------------------
-- Table name: hdset_releases
-- �������� �������
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

COMMENT ON TABLE hdset_releases                           IS '�������� �������';
COMMENT ON COLUMN hdset_releases.release_id               IS '�������������';
COMMENT ON COLUMN hdset_releases.release_code             IS '���';
COMMENT ON COLUMN hdset_releases.release_name             IS '��������';
COMMENT ON COLUMN hdset_releases.release_comment          IS '��������';
COMMENT ON COLUMN hdset_releases.file_cnt                 IS '����� ������';
COMMENT ON COLUMN hdset_releases.success_cnt              IS '����� ������� ����������� ��������';
COMMENT ON COLUMN hdset_releases.error_cnt                IS '����� ��������� ��������';
COMMENT ON COLUMN hdset_releases.status                   IS '������: I,S,E,D,R';
COMMENT ON COLUMN hdset_releases.i_time                   IS '���� � ����� ��������� ������';
COMMENT ON COLUMN hdset_releases.i_user                   IS '��� ���������';
COMMENT ON COLUMN hdset_releases.err_text                 IS '����� ������';

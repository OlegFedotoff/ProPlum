

-------------------------------------------------------------------------
-- Table name: hdset_releases
-- �������� �������
-------------------------------------------------------------------------
CREATE TABLE hdset_releases(
  release_id        NUMBER(16),
  release_code      VARCHAR2(255) NOT NULL,
  release_name      VARCHAR2(255),
  release_comment   VARCHAR2(2000),
  file_cnt          NUMBER(16),
  success_cnt       NUMBER(16),
  error_cnt         NUMBER(16),
  status            VARCHAR2(20),
  i_time            DATE,
  i_user            VARCHAR2(255),
  err_text          VARCHAR2(2000)
)
  TABLESPACE sandbox
;

ALTER TABLE hdset_releases
  ADD CONSTRAINT hdset_releases PRIMARY KEY(release_id) USING INDEX TABLESPACE sandbox;


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

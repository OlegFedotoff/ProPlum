

-------------------------------------------------------------------------
-- Table name: hdset_logs
-- ����������� ���������� �������
-------------------------------------------------------------------------
CREATE TABLE hdset_logs(
  log_id        NUMBER(16),
  release_id    NUMBER(16),
  migration_id  NUMBER(16),
  i_time        DATE,
  i_user        VARCHAR2(255),
  status        VARCHAR2(20),
  action_type   VARCHAR2(20),
  object_type   VARCHAR2(255),
  object_schema VARCHAR2(255),
  object_name   VARCHAR2(255),
  action_info   VARCHAR2(2000),
  error_text    VARCHAR2(2000)
)
  TABLESPACE sandbox
;

ALTER TABLE hdset_logs
  ADD CONSTRAINT pk_hdset_logs PRIMARY KEY(log_id) USING INDEX TABLESPACE sandbox;

CREATE INDEX i_hdset_logs_migration ON hdset_logs(migration_id) TABLESPACE sandbox;
CREATE INDEX i_hdset_logs_release ON hdset_logs(release_id) TABLESPACE sandbox;

COMMENT ON TABLE hdset_logs                           IS '����������� ���������� ������';
COMMENT ON COLUMN hdset_logs.log_id                   IS '�������������';
COMMENT ON COLUMN hdset_logs.release_id               IS '��������';
COMMENT ON COLUMN hdset_logs.migration_id             IS '��������';
COMMENT ON COLUMN hdset_logs.i_time                   IS '����� ';
COMMENT ON COLUMN hdset_logs.i_user                   IS '��� �������� ��������';
COMMENT ON COLUMN hdset_logs.status                   IS '������: S,E';
COMMENT ON COLUMN hdset_logs.action_type              IS '��� ��������: M,R,O';
COMMENT ON COLUMN hdset_logs.object_type              IS '��� �������';
COMMENT ON COLUMN hdset_logs.object_schema            IS '�����';
COMMENT ON COLUMN hdset_logs.object_name              IS '��� �������';
COMMENT ON COLUMN hdset_logs.action_info              IS '�������� ��������';
COMMENT ON COLUMN hdset_logs.error_text               IS '������';

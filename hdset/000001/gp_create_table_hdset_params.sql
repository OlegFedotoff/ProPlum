

-------------------------------------------------------------------------
-- Table name: hdset_params
-- ��������� ����������
-------------------------------------------------------------------------
CREATE TABLE hdset_params(
  code    VARCHAR(255) PRIMARY KEY,
  value   VARCHAR(255)
)
;


COMMENT ON TABLE hdset_params                           IS '��������� ����������';
COMMENT ON COLUMN hdset_params.code                     IS '���';
COMMENT ON COLUMN hdset_params.value                    IS '��������';



INSERT INTO hdset_params(code, value) VALUES('core_version',      '000000');
INSERT INTO hdset_params(code, value) VALUES('migration_version', '000000');

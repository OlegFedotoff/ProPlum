

-------------------------------------------------------------------------
-- Table name: hdset_params
-- Параметры репзитария
-------------------------------------------------------------------------
CREATE TABLE hdset_params(
  code    VARCHAR2(255),
  value   VARCHAR2(255)
)
  TABLESPACE sandbox
;

ALTER TABLE hdset_params
  ADD CONSTRAINT pk_hdset_params PRIMARY KEY(code) USING INDEX TABLESPACE sandbox;


COMMENT ON TABLE hdset_params                           IS 'Параметры репзитария';
COMMENT ON COLUMN hdset_params.code                     IS 'Код';
COMMENT ON COLUMN hdset_params.value                    IS 'Значение';



INSERT INTO hdset_params(code, value) VALUES('core_version',      '000000');
INSERT INTO hdset_params(code, value) VALUES('migration_version', '000000');
COMMIT;
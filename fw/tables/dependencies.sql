-- fw.dependencies определение

-- Drop table

-- DROP FOREIGN TABLE fw.dependencies;

CREATE FOREIGN TABLE fw.dependencies (
	object_id int8 NULL, -- id объекта из objects
	object_id_depend int8 NULL -- id объекта, от которого зависит dependencies.object_id
)
SERVER pg_fw_prod
OPTIONS (table_name 'dependencies');
COMMENT ON FOREIGN TABLE fw.dependencies IS 'Информация о зависимостях при загрузке объектов';

-- Column comments

COMMENT ON COLUMN fw.dependencies.object_id IS 'id объекта из objects';
COMMENT ON COLUMN fw.dependencies.object_id_depend IS 'id объекта, от которого зависит dependencies.object_id';

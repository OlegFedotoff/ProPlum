-- fw.dependencies_bkp_from_pg определение

-- Drop table

-- DROP TABLE fw.dependencies_bkp_from_pg;

CREATE TABLE fw.dependencies_bkp_from_pg (
	object_id int8 NULL,
	object_id_depend int8 NULL
)
DISTRIBUTED REPLICATED;



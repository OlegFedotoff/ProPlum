-- fw.chains определение

-- Drop table

-- DROP FOREIGN TABLE fw.chains;

CREATE FOREIGN TABLE fw.chains (
	chain_name text NOT NULL, -- Name of process chain
	chain_description text NULL, -- Process chain description
	"active" bool NULL, -- Active flag
	schedule text NULL, -- Chain schedule
	job_name text NULL, -- Airflow DAG name
	"sequence" text NULL -- Steps in DAG
)
SERVER pg_fw_prod
OPTIONS (table_name 'chains');
COMMENT ON FOREIGN TABLE fw.chains IS 'Process chains';

-- Column comments

COMMENT ON COLUMN fw.chains.chain_name IS 'Name of process chain';
COMMENT ON COLUMN fw.chains.chain_description IS 'Process chain description';
COMMENT ON COLUMN fw.chains."active" IS 'Active flag';
COMMENT ON COLUMN fw.chains.schedule IS 'Chain schedule';
COMMENT ON COLUMN fw.chains.job_name IS 'Airflow DAG name';
COMMENT ON COLUMN fw.chains."sequence" IS 'Steps in DAG';

-- fw.chains_bkp_from_pg определение

-- Drop table

-- DROP TABLE fw.chains_bkp_from_pg;

CREATE TABLE fw.chains_bkp_from_pg (
	chain_name text NOT NULL,
	chain_description text NULL,
	"active" bool NULL,
	schedule text NULL,
	job_name text NULL,
	"sequence" text NULL
)
DISTRIBUTED REPLICATED;



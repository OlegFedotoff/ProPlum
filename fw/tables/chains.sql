
dev: |
    CREATE FOREIGN TABLE ${target_schema}.chains (
        chain_name text NOT NULL,
        chain_description text,
        "active" bool,
        schedule text,
        job_name text,
        "sequence" text
    )
    SERVER pg_fw_dev
    OPTIONS (table_name 'chains');

prod: |
    CREATE FOREIGN TABLE ${target_schema}.chains (
        chain_name text NOT NULL,
        chain_description text,
        "active" bool,
        schedule text,
        job_name text,
        "sequence" text
    )
    SERVER pg_fw_prod
    OPTIONS (table_name 'chains');

all: |
    COMMENT ON FOREIGN TABLE ${target_schema}.chains IS 'Process chains';
    COMMENT ON COLUMN ${target_schema}.chains.chain_name IS 'Name of process chain';
    COMMENT ON COLUMN ${target_schema}.chains.chain_description IS 'Process chain description';
    COMMENT ON COLUMN ${target_schema}.chains.active IS 'Active flag';
    COMMENT ON COLUMN ${target_schema}.chains.schedule IS 'Chain schedule';
    COMMENT ON COLUMN ${target_schema}.chains.job_name IS 'Airflow DAG name';
    COMMENT ON COLUMN ${target_schema}.chains.sequence IS 'Steps in DAG';


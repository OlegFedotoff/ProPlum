-- fw.partition_run определение

-- Drop table

-- DROP FOREIGN TABLE fw.partition_run;

CREATE FOREIGN TABLE fw.partition_run (
	object_id int8 NOT NULL,
	generation_id int8 NOT NULL,
	command_id int8 NOT NULL,
	attempt int4 NOT NULL,
	airflow_dag_id text NOT NULL,
	airflow_run_id text NOT NULL,
	status text NOT NULL,
	max_parallel int4 NOT NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	finished_at timestamptz NULL,
	error_code text NULL,
	error_text text NULL
)
SERVER pg_fw_prod
OPTIONS (table_name 'partition_run');
COMMENT ON FOREIGN TABLE fw.partition_run IS 'Заголовок текущего поколения очереди dbt-партиций одного object_id строка синхронизации API.';

-- Column comments

COMMENT ON COLUMN fw.partition_run.object_id IS 'Идентификатор объекта и ключ независимой partition-очереди.';
COMMENT ON COLUMN fw.partition_run.generation_id IS 'Уникальный идентификатор текущего поколения очереди.';
COMMENT ON COLUMN fw.partition_run.command_id IS 'Идентификатор связанной команды, сохраняемый как диагностические метаданные.';
COMMENT ON COLUMN fw.partition_run.attempt IS 'Номер попытки связанной команды, сохраняемый как диагностические метаданные.';
COMMENT ON COLUMN fw.partition_run.airflow_dag_id IS 'Идентификатор Airflow DAG, создавшего поколение.';
COMMENT ON COLUMN fw.partition_run.airflow_run_id IS 'Идентификатор Airflow DagRun, создавшего поколение.';
COMMENT ON COLUMN fw.partition_run.status IS 'Состояние поколения: ACTIVE, SUCCESS, ERROR или ABORTED.';
COMMENT ON COLUMN fw.partition_run.max_parallel IS 'Максимальное число одновременно выполняемых partition-элементов.';
COMMENT ON COLUMN fw.partition_run.created_at IS 'Момент создания текущего поколения.';
COMMENT ON COLUMN fw.partition_run.updated_at IS 'Момент последнего изменения заголовка поколения.';
COMMENT ON COLUMN fw.partition_run.finished_at IS 'Момент перехода поколения в терминальное состояние.';
COMMENT ON COLUMN fw.partition_run.error_code IS 'Код ошибки или abort текущего поколения.';
COMMENT ON COLUMN fw.partition_run.error_text IS 'Текст ошибки или abort текущего поколения.';

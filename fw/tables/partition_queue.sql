-- fw.partition_queue определение

-- Drop table

-- DROP FOREIGN TABLE fw.partition_queue;

CREATE FOREIGN TABLE fw.partition_queue (
	object_id int8 NOT NULL,
	generation_id int8 NOT NULL,
	command_id int8 NOT NULL,
	partition_code text NOT NULL,
	start_date date NOT NULL,
	end_date date NOT NULL,
	extra jsonb NULL,
	seq_no int4 NOT NULL,
	queue_position int4 NOT NULL,
	status text NOT NULL,
	claimed_by text NULL,
	started_at timestamptz NULL,
	finished_at timestamptz NULL,
	updated_at timestamptz NOT NULL,
	error_code text NULL,
	error_text text NULL
)
SERVER pg_fw_prod
OPTIONS (table_name 'partition_queue');
COMMENT ON FOREIGN TABLE fw.partition_queue IS 'Элементы текущего поколения последовательной execution queue dbt-партиций.';

-- Column comments

COMMENT ON COLUMN fw.partition_queue.object_id IS 'Идентификатор объекта.';
COMMENT ON COLUMN fw.partition_queue.generation_id IS 'Поколение partition-очереди из partition_run.';
COMMENT ON COLUMN fw.partition_queue.command_id IS 'Идентификатор связанной команды, сохраняемый как диагностические метаданные.';
COMMENT ON COLUMN fw.partition_queue.partition_code IS 'Уникальный код партиции внутри object_id.';
COMMENT ON COLUMN fw.partition_queue.start_date IS 'Включаемая начальная дата интервала партиции.';
COMMENT ON COLUMN fw.partition_queue.end_date IS 'Включаемая конечная дата интервала партиции.';
COMMENT ON COLUMN fw.partition_queue.extra IS 'Дополнительные параметры партиции из рассчитанного JSON-списка.';
COMMENT ON COLUMN fw.partition_queue.seq_no IS 'Порядковый номер после сортировки по start_date и partition_code, начиная с нуля.';
COMMENT ON COLUMN fw.partition_queue.queue_position IS 'Число успешно завершаемых элементов до права запуска партиции.';
COMMENT ON COLUMN fw.partition_queue.status IS 'Состояние элемента: WAITING, RUNNING, SUCCESS, FAILED или CANCELLED.';
COMMENT ON COLUMN fw.partition_queue.claimed_by IS 'Claim token mapped Airflow task, запустившей dbt.';
COMMENT ON COLUMN fw.partition_queue.started_at IS 'Момент успешного атомарного claim.';
COMMENT ON COLUMN fw.partition_queue.finished_at IS 'Момент SUCCESS или FAILED текущей партиции.';
COMMENT ON COLUMN fw.partition_queue.updated_at IS 'Момент последнего изменения элемента.';
COMMENT ON COLUMN fw.partition_queue.error_code IS 'Код ошибки dbt для FAILED-элемента.';
COMMENT ON COLUMN fw.partition_queue.error_text IS 'Текст ошибки dbt для FAILED-элемента.';

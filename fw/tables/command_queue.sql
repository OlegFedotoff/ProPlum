-- fw.command_queue определение

-- Drop table

-- DROP FOREIGN TABLE fw.command_queue;

CREATE FOREIGN TABLE fw.command_queue (
	command_id int8 NOT NULL,
	queue_code text NOT NULL,
	object_id int8 NOT NULL,
	batch_id int8 NULL,
	request_id text NULL,
	status text NOT NULL,
	priority int4 NOT NULL,
	attempt int4 NOT NULL,
	max_attempts int4 NOT NULL,
	available_at timestamptz NOT NULL,
	claimed_at timestamptz NULL,
	claimed_by text NULL,
	claim_token uuid NULL,
	dispatched_at timestamptz NULL,
	finished_at timestamptz NULL,
	worker_dag_id text NULL,
	worker_dag_run_id text NULL,
	load_id int8 NULL,
	payload jsonb NOT NULL,
	force_run bool NOT NULL,
	force_run_reason text NULL,
	error_code text NULL,
	error_text text NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL
)
SERVER pg_fw_prod
OPTIONS (table_name 'command_queue');
COMMENT ON FOREIGN TABLE fw.command_queue IS 'Оперативная очередь команд Airflow.';

-- Column comments

COMMENT ON COLUMN fw.command_queue.command_id IS 'Технический идентификатор команды очереди.';
COMMENT ON COLUMN fw.command_queue.queue_code IS 'Код очереди или этапа обработки.';
COMMENT ON COLUMN fw.command_queue.object_id IS 'Идентификатор объекта из fw.objects.';
COMMENT ON COLUMN fw.command_queue.batch_id IS 'Групповой batch NULL для самостоятельного ручного запуска.';
COMMENT ON COLUMN fw.command_queue.request_id IS 'Идентификатор idempotent самостоятельного ручного запуска NULL для batch.';
COMMENT ON COLUMN fw.command_queue.status IS 'Состояние команды: QUEUED, CLAIMED, RUNNING, SUCCESS, FAILED или CANCELLED.';
COMMENT ON COLUMN fw.command_queue.priority IS 'Приоритет выдачи: большее значение выдаётся раньше.';
COMMENT ON COLUMN fw.command_queue.attempt IS 'Число фактически начатых Worker-попыток.';
COMMENT ON COLUMN fw.command_queue.max_attempts IS 'Максимальное число автоматических Worker-попыток.';
COMMENT ON COLUMN fw.command_queue.available_at IS 'Время, раньше которого команда не может быть выдана Dispatcher.';
COMMENT ON COLUMN fw.command_queue.claimed_at IS 'Момент атомарного claim команды Dispatcher-ом.';
COMMENT ON COLUMN fw.command_queue.claimed_by IS 'Идентификатор Dispatcher, получившего claim.';
COMMENT ON COLUMN fw.command_queue.claim_token IS 'UUID токен текущей попытки, защищающий от устаревших Worker.';
COMMENT ON COLUMN fw.command_queue.dispatched_at IS 'Момент перевода claim в RUNNING Worker-ом.';
COMMENT ON COLUMN fw.command_queue.finished_at IS 'Момент окончательного SUCCESS, FAILED или CANCELLED.';
COMMENT ON COLUMN fw.command_queue.worker_dag_id IS 'DAG Worker, владеющий выполняемой командой.';
COMMENT ON COLUMN fw.command_queue.worker_dag_run_id IS 'Конкретный DagRun Worker, владеющий выполняемой командой.';
COMMENT ON COLUMN fw.command_queue.load_id IS 'Связанный load_id Greenplum, если он требуется обработчику очереди.';
COMMENT ON COLUMN fw.command_queue.payload IS 'Непрозрачные параметры запуска, передаваемые Airflow без валидации API.';
COMMENT ON COLUMN fw.command_queue.force_run IS 'Одноразовый признак запуска без проверки зависимостей.';
COMMENT ON COLUMN fw.command_queue.force_run_reason IS 'Причина ручного force-run.';
COMMENT ON COLUMN fw.command_queue.error_code IS 'Код последней или итоговой ошибки.';
COMMENT ON COLUMN fw.command_queue.error_text IS 'Текст последней или итоговой ошибки.';
COMMENT ON COLUMN fw.command_queue.created_at IS 'Момент создания команды.';
COMMENT ON COLUMN fw.command_queue.updated_at IS 'Момент последнего изменения состояния или служебных полей.';

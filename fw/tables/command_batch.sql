-- fw.command_batch определение

-- Drop table

-- DROP FOREIGN TABLE fw.command_batch;

CREATE FOREIGN TABLE fw.command_batch (
	batch_id int8 NOT NULL,
	load_group text NOT NULL,
	business_date date NULL,
	initial_queue_code text NULL,
	final_queue_code text NOT NULL,
	status text NOT NULL,
	max_duration interval NOT NULL,
	finish_reason text NULL,
	object_count int4 NULL,
	success_count int4 NULL,
	failed_count int4 NULL,
	cancelled_count int4 NULL,
	started_at timestamptz NOT NULL,
	finished_at timestamptz NULL,
	created_by text NOT NULL,
	error_text text NULL
)
SERVER pg_fw_prod
OPTIONS (table_name 'command_batch');
COMMENT ON FOREIGN TABLE fw.command_batch IS 'Жизненный цикл одного группового запуска загрузок: хранит итог и снимок счётчиков.';

-- Column comments

COMMENT ON COLUMN fw.command_batch.batch_id IS 'Технический идентификатор batch.';
COMMENT ON COLUMN fw.command_batch.load_group IS 'Группа объектов, для которой создан batch.';
COMMENT ON COLUMN fw.command_batch.business_date IS 'Бизнес-дата запуска, не является ключом batch.';
COMMENT ON COLUMN fw.command_batch.initial_queue_code IS 'Тип очереди, команды которой создаются при старте batch NULL, если их ставит внешняя система.';
COMMENT ON COLUMN fw.command_batch.final_queue_code IS 'Тип очереди, по командам которой определяется итоговый статус batch.';
COMMENT ON COLUMN fw.command_batch.status IS 'Состояние batch: RUNNING, SUCCESS, FAILED или CANCELLED.';
COMMENT ON COLUMN fw.command_batch.max_duration IS 'Максимально допустимая длительность активного batch.';
COMMENT ON COLUMN fw.command_batch.finish_reason IS 'Причина завершения: COMPLETED, TIMEOUT или FORCED.';
COMMENT ON COLUMN fw.command_batch.object_count IS 'Число активных объектов группы на момент завершения.';
COMMENT ON COLUMN fw.command_batch.success_count IS 'Число объектов с успешной командой финальной очереди на момент завершения.';
COMMENT ON COLUMN fw.command_batch.failed_count IS 'Число объектов с неуспешной командой финальной очереди на момент завершения.';
COMMENT ON COLUMN fw.command_batch.cancelled_count IS 'Число объектов с отменённой командой финальной очереди на момент завершения.';
COMMENT ON COLUMN fw.command_batch.started_at IS 'Момент создания и начала batch.';
COMMENT ON COLUMN fw.command_batch.finished_at IS 'Момент окончательного завершения batch.';
COMMENT ON COLUMN fw.command_batch.created_by IS 'Пользователь PostgreSQL, создавший batch.';
COMMENT ON COLUMN fw.command_batch.error_text IS 'Описание timeout, принудительного завершения или иной итоговой ошибки.';

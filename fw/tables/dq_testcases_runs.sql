-- fw.dq_testcases_runs определение

-- Drop table

-- DROP TABLE fw.dq_testcases_runs;

CREATE TABLE fw.dq_testcases_runs (
	testrun_id int8 NOT NULL, -- id запуска тест-кейса
	testcase_id int8 NOT NULL, -- id тест-кейса
	test_user text NULL, -- Пользователь, запустивший тест-кейс
	testrun_start timestamp NULL, -- Метка времени запуска тест-кейса
	testrun_end timestamp NOT NULL, -- Метка времени окончания выполнения тест-кейса
	testcase_sql text NULL, -- Проверочный запрос
	benchmark_sql text NULL, -- Проверочный эталонный запрос
	row_count int8 NULL, -- Количество несовпадающих строк по результатам выполнения тест-кейса
	test_result json NULL -- Несовпадающие строки в формате json
)
DISTRIBUTED RANDOMLY;
COMMENT ON TABLE fw.dq_testcases_runs IS 'Результаты запуска тест-кейсов';

-- Column comments

COMMENT ON COLUMN fw.dq_testcases_runs.testrun_id IS 'id запуска тест-кейса';
COMMENT ON COLUMN fw.dq_testcases_runs.testcase_id IS 'id тест-кейса';
COMMENT ON COLUMN fw.dq_testcases_runs.test_user IS 'Пользователь, запустивший тест-кейс';
COMMENT ON COLUMN fw.dq_testcases_runs.testrun_start IS 'Метка времени запуска тест-кейса';
COMMENT ON COLUMN fw.dq_testcases_runs.testrun_end IS 'Метка времени окончания выполнения тест-кейса';
COMMENT ON COLUMN fw.dq_testcases_runs.testcase_sql IS 'Проверочный запрос';
COMMENT ON COLUMN fw.dq_testcases_runs.benchmark_sql IS 'Проверочный эталонный запрос';
COMMENT ON COLUMN fw.dq_testcases_runs.row_count IS 'Количество несовпадающих строк по результатам выполнения тест-кейса';
COMMENT ON COLUMN fw.dq_testcases_runs.test_result IS 'Несовпадающие строки в формате json';



-- fw.dq_testcases определение

-- Drop table

-- DROP TABLE fw.dq_testcases;

CREATE TABLE fw.dq_testcases (
	testcase_id int8 NOT NULL, -- id тест-кейса
	testcase_name text NOT NULL, -- Краткое наименование тест-кейса
	testcase_desc text NULL, -- Полное описание тест-кейса
	object_name text NULL, -- Наименование объекта, в котором проверяются данные
	testcase_sql text NULL, -- Проверочный запрос
	benchmark_sql text NULL, -- Проверочный эталонный запрос
	test_group text NULL, -- Группа тест-кейсов
	key_fields _text NULL, -- Список полей, которые являются бизнес-ключем в результатах запроса testcase_sql
	"active" bool NOT NULL, -- Объект активен?
	connect_type text NULL, -- Тип соединения к БД (OracleHook, psycopg2)
	object_id int4 NULL, -- id объекта из таблицы fw.objects
	connect_name text NULL -- Имя соединения в Airflow (oracle54, oracle57)
)
DISTRIBUTED RANDOMLY;
COMMENT ON TABLE fw.dq_testcases IS 'Справочник тест-кейсов, содержит описание сценариев тестирования по качеству данных';

-- Column comments

COMMENT ON COLUMN fw.dq_testcases.testcase_id IS 'id тест-кейса';
COMMENT ON COLUMN fw.dq_testcases.testcase_name IS 'Краткое наименование тест-кейса';
COMMENT ON COLUMN fw.dq_testcases.testcase_desc IS 'Полное описание тест-кейса';
COMMENT ON COLUMN fw.dq_testcases.object_name IS 'Наименование объекта, в котором проверяются данные';
COMMENT ON COLUMN fw.dq_testcases.testcase_sql IS 'Проверочный запрос';
COMMENT ON COLUMN fw.dq_testcases.benchmark_sql IS 'Проверочный эталонный запрос';
COMMENT ON COLUMN fw.dq_testcases.test_group IS 'Группа тест-кейсов';
COMMENT ON COLUMN fw.dq_testcases.key_fields IS 'Список полей, которые являются бизнес-ключем в результатах запроса testcase_sql';
COMMENT ON COLUMN fw.dq_testcases."active" IS 'Объект активен?';
COMMENT ON COLUMN fw.dq_testcases.connect_type IS 'Тип соединения к БД (OracleHook, psycopg2)';
COMMENT ON COLUMN fw.dq_testcases.object_id IS 'id объекта из таблицы fw.objects';
COMMENT ON COLUMN fw.dq_testcases.connect_name IS 'Имя соединения в Airflow (oracle54, oracle57)';



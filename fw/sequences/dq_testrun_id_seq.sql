-- fw.dq_testrun_id_seq определение

-- DROP SEQUENCE fw.dq_testrun_id_seq;

CREATE SEQUENCE fw.dq_testrun_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	NO CYCLE;
COMMENT ON SEQUENCE dq_testrun_id_seq IS 'Ид. запуска тест-кейса';



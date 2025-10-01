-- fw.load_results_id_seq определение

-- DROP SEQUENCE fw.load_results_id_seq;

CREATE SEQUENCE fw.load_results_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	NO CYCLE;

GRANT USAGE, SELECT ON SEQUENCE fw.load_results_id_seq TO role_data_loader;



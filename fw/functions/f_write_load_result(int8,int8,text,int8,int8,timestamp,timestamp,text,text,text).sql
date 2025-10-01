-- DROP FUNCTION fw.f_write_load_result(int8,int8,text,int8,int8,timestamp,timestamp,text,text,text);

CREATE OR REPLACE FUNCTION fw.f_write_load_result(
	p_load_id int8,
	p_object_id int8,
	p_table_name text,
	p_rows_affected int8 DEFAULT 0,
	p_duration_ms int8 DEFAULT NULL,
	p_started_at timestamp DEFAULT NULL,
	p_finished_at timestamp DEFAULT NULL,
	p_status text DEFAULT 'SUCCESS',
	p_error_message text DEFAULT NULL,
	p_extra text DEFAULT NULL
)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$


/*Write load result into fw.load_results*/
declare
	v_location   text := 'fw.f_write_load_result';
	v_result_id  int8;
begin
	-- Generate id
	v_result_id := nextval('fw.load_results_id_seq');

	-- Insert actual row
	INSERT INTO fw.load_results (
		result_id,
		load_id,
		object_id,
		table_name,
		rows_affected,
		duration_ms,
		started_at,
		finished_at,
		status,
		error_message,
		extra
	) VALUES (
		v_result_id,
		p_load_id,
		p_object_id,
		p_table_name,
		COALESCE(p_rows_affected, 0),
		p_duration_ms,
		COALESCE(p_started_at, now()),
		p_finished_at,
		COALESCE(p_status, 'SUCCESS'),
		p_error_message,
		p_extra
	);

	return v_result_id;
end;


$$
EXECUTE ON ANY;

GRANT EXECUTE ON FUNCTION fw.f_write_load_result(int8,int8,text,int8,int8,timestamp,timestamp,text,text,text) TO role_data_loader;



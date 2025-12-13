-- Wrapper without SECURITY DEFINER: writes to ext_tables_params_log using invoker identity

CREATE OR REPLACE FUNCTION fw.f_save_ext_tables_params(
	p_param_row fw.ext_tables_params
)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_row fw.ext_tables_params;
	v_change text;
    v_res record;
BEGIN
	SELECT * INTO v_res FROM fw.f_save_ext_tables_params_core(p_param_row);
	v_row := v_res.o_row;
	v_change := v_res.o_change_type;

	INSERT INTO fw.ext_tables_params_log (
		object_id,
		load_method,
		connection_string,
		additional,
		"active",
		change_type,
		change_timestamp,
		change_username
	) VALUES (
		v_row.object_id,
		v_row.load_method,
		v_row.connection_string,
		v_row.additional,
		v_row."active",
		v_change,
		current_timestamp,
		session_user
	);
	RETURN;
END;
$$
EXECUTE ON MASTER;

GRANT EXECUTE ON FUNCTION fw.f_save_ext_tables_params(fw.ext_tables_params) TO role_data_loader;




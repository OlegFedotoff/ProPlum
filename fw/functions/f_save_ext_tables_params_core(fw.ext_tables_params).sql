-- Core upsert function for ext_tables_params (SECURITY DEFINER). Called by wrapper for logging.

CREATE OR REPLACE FUNCTION fw.f_save_ext_tables_params_core(
	p_param_row fw.ext_tables_params,
	OUT o_row fw.ext_tables_params,
	OUT o_change_type text
)
	RETURNS record
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
DECLARE
	v_prev fw.ext_tables_params;
BEGIN
	IF p_param_row.object_id IS NULL THEN
		RAISE EXCEPTION 'object_id is mandatory and cannot be NULL';
	END IF;
	IF p_param_row.load_method IS NULL OR p_param_row.load_method = '' THEN
		RAISE EXCEPTION 'load_method is mandatory and cannot be NULL or empty';
	END IF;

	-- Ищем существующую запись по object_id и load_method
	SELECT * INTO v_prev 
	FROM fw.ext_tables_params 
	WHERE object_id = p_param_row.object_id 
	  AND load_method = p_param_row.load_method;
	
	IF FOUND THEN
		o_change_type := 'UPDATE';
		UPDATE fw.ext_tables_params
		   SET connection_string = COALESCE(p_param_row.connection_string, v_prev.connection_string),
		       additional        = COALESCE(p_param_row.additional, v_prev.additional),
		       "active"          = COALESCE(p_param_row."active", v_prev."active")
		 WHERE object_id = p_param_row.object_id 
		   AND load_method = p_param_row.load_method;

		o_row.object_id         := p_param_row.object_id;
		o_row.load_method       := p_param_row.load_method;
		o_row.connection_string := COALESCE(p_param_row.connection_string, v_prev.connection_string);
		o_row.additional        := COALESCE(p_param_row.additional, v_prev.additional);
		o_row."active"          := COALESCE(p_param_row."active", v_prev."active");
	ELSE
		o_change_type := 'INSERT';
		INSERT INTO fw.ext_tables_params (
			object_id,
			load_method,
			connection_string,
			additional,
			"active"
		) VALUES (
			p_param_row.object_id,
			p_param_row.load_method,
			p_param_row.connection_string,
			p_param_row.additional,
			p_param_row."active"
		);

		o_row := p_param_row;
	END IF;
	RETURN;
END;
$$
EXECUTE ON MASTER;




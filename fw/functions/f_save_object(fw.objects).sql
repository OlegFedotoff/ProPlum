-- Wrapper without SECURITY DEFINER: writes to objects_log using invoker identity

CREATE OR REPLACE FUNCTION fw.f_save_object(
	p_object_row fw.objects
)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_row fw.objects;
	v_change text;
    v_res record;
BEGIN
	SELECT * INTO v_res FROM fw.f_save_object_core(p_object_row);
	v_row := v_res.o_row;
	v_change := v_res.o_change_type;

	INSERT INTO fw.objects_log (
		object_id,
		object_name,
		object_desc,
		extraction_type,
		load_type,
		merge_key,
		delta_field,
		delta_field_format,
		delta_safety_period,
		bdate_field,
		bdate_field_format,
		bdate_safety_period,
		load_method,
		job_name,
		responsible_mail,
		priority,
		periodicity,
		load_interval,
		activitystart,
		activityend,
		"active",
		load_start_date,
		delta_start_date,
		delta_mode,
		connect_string,
		load_function_name,
		where_clause,
		load_group,
		src_date_type,
		src_ts_type,
		column_name_mapping,
		transform_mapping,
		delta_field_type,
		bdate_field_type,
		change_type,
		change_timestamp,
		change_username
	) VALUES (
		v_row.object_id,
		v_row.object_name,
		v_row.object_desc,
		v_row.extraction_type,
		v_row.load_type,
		v_row.merge_key,
		v_row.delta_field,
		v_row.delta_field_format,
		v_row.delta_safety_period,
		v_row.bdate_field,
		v_row.bdate_field_format,
		v_row.bdate_safety_period,
		v_row.load_method,
		v_row.job_name,
		v_row.responsible_mail,
		v_row.priority,
		v_row.periodicity,
		v_row.load_interval,
		v_row.activitystart,
		v_row.activityend,
		v_row."active",
		v_row.load_start_date,
		v_row.delta_start_date,
		v_row.delta_mode,
		v_row.connect_string,
		v_row.load_function_name,
		v_row.where_clause,
		v_row.load_group,
		v_row.src_date_type,
		v_row.src_ts_type,
		v_row.column_name_mapping,
		v_row.transform_mapping,
		v_row.delta_field_type,
		v_row.bdate_field_type,
		v_change,
		current_timestamp,
		session_user
	);
	RETURN;
END;
$$
EXECUTE ON MASTER;

GRANT EXECUTE ON FUNCTION fw.f_save_object(fw.objects) TO role_data_loader;



-- Core upsert function (SECURITY DEFINER). Called by wrapper for logging.

CREATE OR REPLACE FUNCTION fw.f_save_object_core(
	p_object_row fw.objects,
	OUT o_row fw.objects,
	OUT o_change_type text
)
	RETURNS record
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
DECLARE
	v_prev fw.objects;
BEGIN
	IF p_object_row.object_id IS NULL THEN
		RAISE EXCEPTION 'object_id is mandatory and cannot be NULL';
	END IF;
	IF p_object_row.object_name IS NULL OR p_object_row.object_name = '' THEN
		RAISE EXCEPTION 'object_name is mandatory and cannot be NULL or empty';
	END IF;

	SELECT * INTO v_prev FROM fw.objects WHERE object_id = p_object_row.object_id;
	IF FOUND THEN
		o_change_type := 'UPDATE';
		UPDATE fw.objects
		   SET object_name          = p_object_row.object_name,
		       object_desc          = COALESCE(p_object_row.object_desc, v_prev.object_desc),
		       extraction_type      = COALESCE(p_object_row.extraction_type, v_prev.extraction_type),
		       load_type            = COALESCE(p_object_row.load_type, v_prev.load_type),
		       merge_key            = COALESCE(p_object_row.merge_key, v_prev.merge_key),
		       delta_field          = COALESCE(p_object_row.delta_field, v_prev.delta_field),
		       delta_field_format   = COALESCE(p_object_row.delta_field_format, v_prev.delta_field_format),
		       delta_safety_period  = COALESCE(p_object_row.delta_safety_period, v_prev.delta_safety_period),
		       bdate_field          = COALESCE(p_object_row.bdate_field, v_prev.bdate_field),
		       bdate_field_format   = COALESCE(p_object_row.bdate_field_format, v_prev.bdate_field_format),
		       bdate_safety_period  = COALESCE(p_object_row.bdate_safety_period, v_prev.bdate_safety_period),
		       load_method          = COALESCE(p_object_row.load_method, v_prev.load_method),
		       job_name             = COALESCE(p_object_row.job_name, v_prev.job_name),
		       responsible_mail     = COALESCE(p_object_row.responsible_mail, v_prev.responsible_mail),
		       priority             = COALESCE(p_object_row.priority, v_prev.priority),
		       periodicity          = COALESCE(p_object_row.periodicity, v_prev.periodicity),
		       load_interval        = COALESCE(p_object_row.load_interval, v_prev.load_interval),
		       activitystart        = COALESCE(p_object_row.activitystart, v_prev.activitystart),
		       activityend          = COALESCE(p_object_row.activityend, v_prev.activityend),
		       "active"            = COALESCE(p_object_row."active", v_prev."active"),
		       load_start_date      = COALESCE(p_object_row.load_start_date, v_prev.load_start_date),
		       delta_start_date     = COALESCE(p_object_row.delta_start_date, v_prev.delta_start_date),
		       delta_mode           = COALESCE(p_object_row.delta_mode, v_prev.delta_mode),
		       connect_string       = COALESCE(p_object_row.connect_string, v_prev.connect_string),
		       load_function_name   = COALESCE(p_object_row.load_function_name, v_prev.load_function_name),
		       where_clause         = COALESCE(p_object_row.where_clause, v_prev.where_clause),
		       load_group           = COALESCE(p_object_row.load_group, v_prev.load_group),
		       src_date_type        = COALESCE(p_object_row.src_date_type, v_prev.src_date_type),
		       src_ts_type          = COALESCE(p_object_row.src_ts_type, v_prev.src_ts_type),
		       column_name_mapping  = COALESCE(p_object_row.column_name_mapping, v_prev.column_name_mapping),
		       transform_mapping    = COALESCE(p_object_row.transform_mapping, v_prev.transform_mapping),
		       delta_field_type     = COALESCE(p_object_row.delta_field_type, v_prev.delta_field_type),
		       bdate_field_type     = COALESCE(p_object_row.bdate_field_type, v_prev.bdate_field_type),
		       param_list           = COALESCE(p_object_row.param_list, v_prev.param_list)
		 WHERE object_id = p_object_row.object_id;

		o_row.object_id           := p_object_row.object_id;
		o_row.object_name         := p_object_row.object_name;
		o_row.object_desc         := COALESCE(p_object_row.object_desc, v_prev.object_desc);
		o_row.extraction_type     := COALESCE(p_object_row.extraction_type, v_prev.extraction_type);
		o_row.load_type           := COALESCE(p_object_row.load_type, v_prev.load_type);
		o_row.merge_key           := COALESCE(p_object_row.merge_key, v_prev.merge_key);
		o_row.delta_field         := COALESCE(p_object_row.delta_field, v_prev.delta_field);
		o_row.delta_field_format  := COALESCE(p_object_row.delta_field_format, v_prev.delta_field_format);
		o_row.delta_safety_period := COALESCE(p_object_row.delta_safety_period, v_prev.delta_safety_period);
		o_row.bdate_field         := COALESCE(p_object_row.bdate_field, v_prev.bdate_field);
		o_row.bdate_field_format  := COALESCE(p_object_row.bdate_field_format, v_prev.bdate_field_format);
		o_row.bdate_safety_period := COALESCE(p_object_row.bdate_safety_period, v_prev.bdate_safety_period);
		o_row.load_method         := COALESCE(p_object_row.load_method, v_prev.load_method);
		o_row.job_name            := COALESCE(p_object_row.job_name, v_prev.job_name);
		o_row.responsible_mail    := COALESCE(p_object_row.responsible_mail, v_prev.responsible_mail);
		o_row.priority            := COALESCE(p_object_row.priority, v_prev.priority);
		o_row.periodicity         := COALESCE(p_object_row.periodicity, v_prev.periodicity);
		o_row.load_interval       := COALESCE(p_object_row.load_interval, v_prev.load_interval);
		o_row.activitystart       := COALESCE(p_object_row.activitystart, v_prev.activitystart);
		o_row.activityend         := COALESCE(p_object_row.activityend, v_prev.activityend);
		o_row."active"           := COALESCE(p_object_row."active", v_prev."active");
		o_row.load_start_date     := COALESCE(p_object_row.load_start_date, v_prev.load_start_date);
		o_row.delta_start_date    := COALESCE(p_object_row.delta_start_date, v_prev.delta_start_date);
		o_row.delta_mode          := COALESCE(p_object_row.delta_mode, v_prev.delta_mode);
		o_row.connect_string      := COALESCE(p_object_row.connect_string, v_prev.connect_string);
		o_row.load_function_name  := COALESCE(p_object_row.load_function_name, v_prev.load_function_name);
		o_row.where_clause        := COALESCE(p_object_row.where_clause, v_prev.where_clause);
		o_row.load_group          := COALESCE(p_object_row.load_group, v_prev.load_group);
		o_row.src_date_type       := COALESCE(p_object_row.src_date_type, v_prev.src_date_type);
		o_row.src_ts_type         := COALESCE(p_object_row.src_ts_type, v_prev.src_ts_type);
		o_row.column_name_mapping := COALESCE(p_object_row.column_name_mapping, v_prev.column_name_mapping);
		o_row.transform_mapping   := COALESCE(p_object_row.transform_mapping, v_prev.transform_mapping);
		o_row.delta_field_type    := COALESCE(p_object_row.delta_field_type, v_prev.delta_field_type);
		o_row.bdate_field_type    := COALESCE(p_object_row.bdate_field_type, v_prev.bdate_field_type);
		o_row.param_list          := COALESCE(p_object_row.param_list, v_prev.param_list);
	ELSE
		o_change_type := 'INSERT';
		INSERT INTO fw.objects (
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
			param_list
		) VALUES (
			p_object_row.object_id,
			p_object_row.object_name,
			p_object_row.object_desc,
			p_object_row.extraction_type,
			p_object_row.load_type,
			p_object_row.merge_key,
			p_object_row.delta_field,
			p_object_row.delta_field_format,
			p_object_row.delta_safety_period,
			p_object_row.bdate_field,
			p_object_row.bdate_field_format,
			p_object_row.bdate_safety_period,
			p_object_row.load_method,
			p_object_row.job_name,
			p_object_row.responsible_mail,
			p_object_row.priority,
			p_object_row.periodicity,
			p_object_row.load_interval,
			p_object_row.activitystart,
			p_object_row.activityend,
			p_object_row."active",
			p_object_row.load_start_date,
			p_object_row.delta_start_date,
			p_object_row.delta_mode,
			p_object_row.connect_string,
			p_object_row.load_function_name,
			p_object_row.where_clause,
			p_object_row.load_group,
			p_object_row.src_date_type,
			p_object_row.src_ts_type,
			p_object_row.column_name_mapping,
			p_object_row.transform_mapping,
			p_object_row.delta_field_type,
			p_object_row.bdate_field_type,
			p_object_row.param_list
		);

		o_row := p_object_row;
	END IF;
	RETURN;
END;
$$
EXECUTE ON MASTER;



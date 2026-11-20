-- DROP FUNCTION fw.f_cancel_reload(int8);

CREATE OR REPLACE FUNCTION fw.f_cancel_reload(p_object_id int8)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$


/*Cancels reload requested by fw.f_request_reload.
 * Sets error status (-1) to last load_id of object if it is NEW (1) or IN_PROCESS (2) and has job_name = 'RELOAD'.
 * Regular loads are never cancelled by this function. Returns cancelled load_id.
 * Partition task already running in Airflow is not interrupted; remaining partition tasks are skipped*/
DECLARE
  v_location        text := 'fw.f_cancel_reload';
  c_reload_job_name text := 'RELOAD';
  v_load_id         int8;
  v_job_name        text;
  v_extraction_from timestamp;
  v_extraction_to   timestamp;
BEGIN
  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'START cancel reload for object '||p_object_id,
     p_location    := v_location);

  v_load_id := fw.f_get_load_id_new_or_inproc(p_object_id := p_object_id);
  if v_load_id is null then
     raise exception 'Object % has no active load_id, nothing to cancel', p_object_id;
  end if;

  select job_name, extraction_from, extraction_to
    into v_job_name, v_extraction_from, v_extraction_to
    from fw.load_info
   where load_id = v_load_id;

  if coalesce(v_job_name, '') <> c_reload_job_name then
     raise exception 'Active load_id % of object % is a regular load, not a reload; it was not cancelled',
       v_load_id, p_object_id;
  end if;

  perform fw.f_set_load_id_error(p_load_id := v_load_id);

  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'END cancel reload for object '||p_object_id||', load_id is: '||v_load_id,
     p_location    := v_location,
     p_load_id     := v_load_id);

  raise notice 'Reload cancelled: object %, load_id %, window [%, %). Partition task already running in Airflow will finish, remaining partitions will be skipped.',
    p_object_id, v_load_id, v_extraction_from::date, v_extraction_to::date;

  return v_load_id;
END;


$$
EXECUTE ON ANY;

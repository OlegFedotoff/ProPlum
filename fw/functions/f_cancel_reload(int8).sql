-- DROP FUNCTION fw.f_cancel_reload(int8);

CREATE OR REPLACE FUNCTION fw.f_cancel_reload(p_object_id int8)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$


/*Cancels reload requested by fw.f_request_reload.
 * Sets error status (-1) to load_id of object request if it is still NEW (1) or IN_PROCESS (2);
 * fw.f_set_load_id_error then removes request row from fw.object_reload (PostgreSQL).
 * If load_id is already finished, only removes request row.
 * Without request row falls back to last load_id of object if it is NEW/IN_PROCESS and has job_name = 'RELOAD'.
 * Regular loads are never cancelled by this function. Returns reload load_id.
 * Delete is not autonomous: commit is required (autocommit in client).
 * Partition task already running in Airflow is not interrupted; after DAG re-parse Airflow returns to regular partitions*/
DECLARE
  v_location        text := 'fw.f_cancel_reload';
  c_reload_job_name text := 'RELOAD';
  v_load_id         int8;
  v_has_request     bool;
  v_load_status     int;
  v_job_name        text;
  v_extraction_from timestamp;
  v_extraction_to   timestamp;
BEGIN
  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'START cancel reload for object '||p_object_id,
     p_location    := v_location);

  select load_id
    into v_load_id
    from fw.object_reload
   where object_id = p_object_id;
  v_has_request := v_load_id is not null;

  if not v_has_request then
     v_load_id := fw.f_get_load_id_new_or_inproc(p_object_id := p_object_id);
     if v_load_id is null then
        raise exception 'Object % has no reload request and no active load_id, nothing to cancel', p_object_id;
     end if;
  end if;

  select load_status, job_name, extraction_from, extraction_to
    into v_load_status, v_job_name, v_extraction_from, v_extraction_to
    from fw.load_info
   where load_id = v_load_id;

  if not v_has_request and coalesce(v_job_name, '') <> c_reload_job_name then
     raise exception 'Active load_id % of object % is a regular load, not a reload; it was not cancelled',
       v_load_id, p_object_id;
  end if;

  if v_load_status in (1, 2) and v_job_name = c_reload_job_name then
     -- request row is removed by fw.f_set_load_id_error; it is SECURITY DEFINER and uses its own postgres_fdw
     -- connection, so deleting the row here as well would make that connection wait for this transaction forever
     perform fw.f_set_load_id_error(p_load_id := v_load_id);
     raise notice 'Reload cancelled: object %, load_id %, window [%, %). Partition task already running in Airflow will finish; after DAG re-parse Airflow returns to regular partitions.',
       p_object_id, v_load_id, v_extraction_from::date, v_extraction_to::date;
  else
     if v_has_request then
        delete from fw.object_reload where object_id = p_object_id;
     end if;
     raise notice 'Reload request removed: object %, load_id % is not active (load_status %), its status was not changed.',
       p_object_id, v_load_id, coalesce(v_load_status::text, '{not found}');
  end if;

  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'END cancel reload for object '||p_object_id||', load_id is: '||v_load_id||', load_status was: '||coalesce(v_load_status::text, '{not found}'),
     p_location    := v_location,
     p_load_id     := v_load_id);

  return v_load_id;
END;


$$
EXECUTE ON ANY;

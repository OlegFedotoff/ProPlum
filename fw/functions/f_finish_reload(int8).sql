-- DROP FUNCTION fw.f_finish_reload(int8);

CREATE OR REPLACE FUNCTION fw.f_finish_reload(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$


/*Finishes reload requested by fw.f_request_reload: removes request row of p_load_id from fw.object_reload (PostgreSQL).
 * Called by Airflow at the end of reload run (after dbt_cleanup): Airflow builds partition tasks of the run
 * from the request, so it must exist until the run ends.
 * Removes request only if p_load_id is a reload (job_name = 'RELOAD') and is finished (load_status 3 or -1).
 * Never changes load_id status. Returns true if request row was removed*/
DECLARE
  v_location        text := 'fw.f_finish_reload';
  c_reload_job_name text := 'RELOAD';
  v_load_status     int;
  v_job_name        text;
  v_object_id       int8;
  v_cnt             int;
BEGIN
  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'START finish reload for load_id = '||coalesce(p_load_id::text, '{empty}'),
     p_location    := v_location,
     p_load_id     := p_load_id);

  select load_status, job_name, object_id
    into v_load_status, v_job_name, v_object_id
    from fw.load_info
   where load_id = p_load_id;

  if v_job_name is distinct from c_reload_job_name then
     perform fw.f_write_log(
        p_log_type    := 'WARN',
        p_log_message := 'load_id = '||coalesce(p_load_id::text, '{empty}')||' is not a reload (job_name = '||coalesce(v_job_name, '{not found}')||'), request not removed',
        p_location    := v_location,
        p_load_id     := p_load_id);
     return false;
  end if;

  if v_load_status is null or v_load_status not in (3, -1) then
     perform fw.f_write_log(
        p_log_type    := 'WARN',
        p_log_message := 'Reload load_id = '||p_load_id||' of object '||v_object_id||' is still active (load_status '||coalesce(v_load_status::text, '{empty}')||'), request not removed',
        p_location    := v_location,
        p_load_id     := p_load_id);
     return false;
  end if;

  delete from fw.object_reload where load_id = p_load_id;
  get diagnostics v_cnt = row_count;

  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'END finish reload for object '||v_object_id||', load_id = '||p_load_id||', load_status '||v_load_status||', request rows removed: '||v_cnt,
     p_location    := v_location,
     p_load_id     := p_load_id);

  return v_cnt > 0;
END;


$$
EXECUTE ON ANY;

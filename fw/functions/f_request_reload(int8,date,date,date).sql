-- DROP FUNCTION fw.f_request_reload(int8, date, date, date);

DROP FUNCTION IF EXISTS fw.f_request_reload(int8, date, date);

CREATE OR REPLACE FUNCTION fw.f_request_reload(p_object_id int8, p_start_date date, p_end_date date, p_valid_to date DEFAULT NULL)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$


/*Requests reload of dbt PARTITION object for window [p_start_date, p_end_date).
 * Creates new load_id with job_name = 'RELOAD' and request row in fw.object_reload (PostgreSQL).
 * Airflow builds partition tasks from fw.object_reload while current date <= valid_to;
 * data is loaded by Airflow, not by this function. Request row is removed on load_id finish or by fw.f_cancel_reload.
 * p_valid_to: last day the request is in effect, current date by default.
 * Window bounds must be aligned to object load_interval (first day of month for '1 mon').
 * Inserts are not autonomous: commit is required (autocommit in client).
 * Cancel: fw.f_cancel_reload(p_object_id)*/
DECLARE
  v_location        text := 'fw.f_request_reload';
  c_reload_job_name text := 'RELOAD';
  c_new_status      int  := 1;
  v_object_id       int8;
  v_object_name     text;
  v_load_method     text;
  v_load_type       text;
  v_extraction_type text;
  v_load_interval   interval;
  v_step_months     int;
  v_step_days       int;
  v_partition_cnt   int;
  v_valid_to        date;
  v_active_load_id  int8;
  v_request_load_id int8;
  v_load_id         int8;
BEGIN
  v_valid_to := coalesce(p_valid_to, current_date);

  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'START request reload for object '||p_object_id||', window ['||coalesce(p_start_date::text,'{empty}')||', '||coalesce(p_end_date::text,'{empty}')||'), valid to '||v_valid_to,
     p_location    := v_location);

  select object_id, object_name, load_method, load_type, extraction_type, load_interval
    into v_object_id, v_object_name, v_load_method, v_load_type, v_extraction_type, v_load_interval
    from fw.objects
   where object_id = p_object_id;

  if v_object_id is null then
     raise exception 'Object % not found in fw.objects', p_object_id;
  end if;
  if lower(coalesce(v_load_method, '')) <> 'dbt' or coalesce(v_load_type, '') <> 'PARTITION' then
     raise exception 'Reload is supported only for dbt PARTITION objects; object % has load_method = %, load_type = %',
       p_object_id, v_load_method, v_load_type;
  end if;
  if p_start_date is null or p_end_date is null or p_start_date >= p_end_date then
     raise exception 'Invalid reload window [%, %) for object %', p_start_date, p_end_date, p_object_id;
  end if;
  if v_valid_to < current_date then
     raise exception 'Invalid valid_to % for object %: must not be earlier than current date %', v_valid_to, p_object_id, current_date;
  end if;

  -- load_interval must be whole months/years or whole days (same rule as Airflow partition builder)
  v_step_months := (date_part('year', v_load_interval) * 12 + date_part('month', v_load_interval))::int;
  v_step_days   := date_part('day', v_load_interval)::int;
  if v_load_interval is null
     or (v_step_months > 0) = (v_step_days > 0)
     or v_load_interval <> make_interval(months := v_step_months, days := v_step_days) then
     raise exception 'Unsupported load_interval % for object %: expected whole months/years or whole days',
       v_load_interval, p_object_id;
  end if;

  -- monthly partitions are exchanged as a whole, so bounds must lie on the partition grid
  if v_step_months > 0 then
     if extract(day from p_start_date) <> 1 or extract(day from p_end_date) <> 1
        or (extract(year from p_start_date)::int * 12 + extract(month from p_start_date)::int - 1) % v_step_months <> 0
        or (extract(year from p_end_date)::int * 12 + extract(month from p_end_date)::int - 1) % v_step_months <> 0 then
        raise exception 'Reload window [%, %) for object % must be aligned to load_interval %',
          p_start_date, p_end_date, p_object_id, v_load_interval;
     end if;
     v_partition_cnt := ((extract(year from p_end_date)::int - extract(year from p_start_date)::int) * 12
                        + extract(month from p_end_date)::int - extract(month from p_start_date)::int) / v_step_months;
  else
     v_partition_cnt := ceil((p_end_date - p_start_date)::numeric / v_step_days)::int;
  end if;

  select load_id
    into v_request_load_id
    from fw.object_reload
   where object_id = p_object_id;
  if v_request_load_id is not null then
     raise exception 'Object % already has reload request with load_id %. Cancel it before requesting a new one: select fw.f_cancel_reload(%);',
       p_object_id, v_request_load_id, p_object_id;
  end if;

  v_active_load_id := fw.f_get_load_id_new_or_inproc(p_object_id := p_object_id);
  if v_active_load_id is not null then
     raise exception 'Object % has active load_id % (NEW or IN_PROCESS). Wait for it to finish or cancel it before requesting reload',
       p_object_id, v_active_load_id;
  end if;

  v_load_id := nextval('fw.load_id_seq');
  insert into fw.load_info
    (load_id, extraction_type, load_type, object_id, load_status, extraction_from, extraction_to, load_method, job_name)
  values
    (v_load_id, v_extraction_type, v_load_type, p_object_id, c_new_status,
     p_start_date, p_end_date, v_load_method, c_reload_job_name);

  insert into fw.object_reload
    (object_id, load_id, reload_from, reload_to, valid_to, requested_by)
  values
    (p_object_id, v_load_id, p_start_date, p_end_date, v_valid_to, session_user);

  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'END request reload for object '||p_object_id||', load_id is: '||v_load_id||', window ['||p_start_date||', '||p_end_date||'), partitions: '||v_partition_cnt||', valid to '||v_valid_to,
     p_location    := v_location,
     p_load_id     := v_load_id);

  raise notice 'Reload requested: object % (%), load_id %, window [%, %), % partitions (load_interval %), valid to %. Airflow will pick it up after DAG re-parse. Cancel: select fw.f_cancel_reload(%);',
    p_object_id, v_object_name, v_load_id, p_start_date, p_end_date, v_partition_cnt, v_load_interval, v_valid_to, p_object_id;

  return v_load_id;
END;


$$
EXECUTE ON ANY;

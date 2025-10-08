-- DROP FUNCTION fw.f_get_load_id_new_or_inproc(int8, date, date);

CREATE OR REPLACE FUNCTION fw.f_get_load_id_new_or_inproc(p_object_id int8, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$


/*Function returns last load_id for object_id if its status is NEW (1) or IN_PROCESS (2)
 * If last load_id has different status or no records exist - returns NULL
 * Parameters p_start_date and p_end_date are kept for backward compatibility but not used*/
DECLARE
  v_location text := 'fw.f_get_load_id_new_or_inproc';
  v_sql text;
  v_load_id_new_status   int  := 1;
  v_load_id_work_status  int  := 2;
  v_load_id              int8;
  v_load_status          int;
begin
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Start get NEW/IN_PROCESS load_id for object ' || p_object_id, 
       p_location    := v_location); 

    -- find last load_id for object
    v_sql := 'select load_id, load_status from fw.load_info 
               where object_id = ' || p_object_id::text ||
             ' order by load_id desc limit 1';
    execute v_sql into v_load_id, v_load_status;

    -- check if last load_id has status NEW (1) or IN_PROCESS (2)
    if v_load_id is not null and v_load_status in (v_load_id_new_status, v_load_id_work_status) then
     perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Find load_id (NEW/IN_PROCESS) for object ' || p_object_id, 
       p_location    := v_location,
       p_load_id     := v_load_id);
    else
     perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'No active (NEW/IN_PROCESS) load_id for object ' || p_object_id, 
       p_location    := v_location);
     v_load_id := null;  -- reset load_id if status is not suitable
    end if;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END get load_id for object ' || p_object_id||', load_id is: '||coalesce(v_load_id::text,'{empty}'), 
       p_location    := v_location); 
    return v_load_id;
END;


$$
EXECUTE ON ANY;



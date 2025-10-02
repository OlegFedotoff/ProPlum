-- DROP FUNCTION fw.f_get_load_id_new_or_inproc(int8, date, date);

CREATE OR REPLACE FUNCTION fw.f_get_load_id_new_or_inproc(p_object_id int8, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$


/*Function returns load_id with status NEW (1) or IN_PROCESS (2) for dates in parameters
 * if dates empty - return load_id with max date*/
DECLARE
  v_location text := 'fw.f_get_load_id_new_or_inproc';
  v_sql text;
  v_load_id_new_status   int  := 1;
  v_load_id_work_status  int  := 2;
  v_load_id              int8;
  v_extr_start           timestamp;
  v_extr_end             timestamp;
  c_minDate              date := to_date('19000101', 'YYYYMMDD');
  c_maxDate              date := to_date('99991231', 'YYYYMMDD');
begin
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Start get NEW/IN_PROCESS load_id for object ' || p_object_id, 
       p_location    := v_location); 

    v_extr_start = coalesce(p_start_date,c_minDate);
    v_extr_end   = coalesce(p_end_date,c_maxDate);
    -- look for existing load_id for load (status 1 or 2)
    v_sql := 'select load_id from fw.load_info 
               where object_id = ' || p_object_id::text ||
             ' and load_status in (' || v_load_id_new_status::text || ',' || v_load_id_work_status::text || ')
               and extraction_from >= '''|| v_extr_start||'''::timestamp and extraction_to <= '''||v_extr_end||'''::timestamp order by load_id desc limit 1';
    execute v_sql into v_load_id;

    if v_load_id is not null then
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
    end if;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END get load_id for object ' || p_object_id||', load_id is: '||coalesce(v_load_id::text,'{empty}'), 
       p_location    := v_location); 
    return v_load_id;
END;


$$
EXECUTE ON ANY;



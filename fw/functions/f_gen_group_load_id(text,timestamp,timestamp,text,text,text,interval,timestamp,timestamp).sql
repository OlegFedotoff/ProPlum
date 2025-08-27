-- DROP FUNCTION fw.f_gen_group_load_id(text, timestamp, timestamp, text, text, text, interval, timestamp, timestamp);

CREATE OR REPLACE FUNCTION fw.f_gen_group_load_id(p_load_group text, p_start_extr timestamp DEFAULT NULL::timestamp without time zone, p_end_extr timestamp DEFAULT NULL::timestamp without time zone, p_extraction_type text DEFAULT NULL::text, p_load_type text DEFAULT NULL::text, p_delta_mode text DEFAULT NULL::text, p_load_interval interval DEFAULT NULL::interval, p_start_load timestamp DEFAULT NULL::timestamp without time zone, p_end_load timestamp DEFAULT NULL::timestamp without time zone)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

/*Function generates load_id for objects in load group*/
DECLARE
    v_location text := 'fw.f_gen_group_load_id';
    v_object_id int8;
    rec record;
    v_sql text;
    v_load_id int8;
BEGIN	

    perform fw.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'START Generate load_id for load_group '||p_load_group, 
       p_location    := v_location); --log function call

  FOR rec IN
      SELECT object_id FROM fw.objects o WHERE o.load_group = p_load_group and o.active
  loop
      v_sql = 'select fw.f_gen_load_id(
                  p_object_id := '||rec.object_id||',
                  p_start_extr := '||coalesce(''''||p_start_extr||'''::timestamp','null::timestamp')||',
                  p_end_extr := '||coalesce(''''||p_end_extr||'''::timestamp','null::timestamp')||',
                  p_extraction_type := '||coalesce(''''||p_extraction_type||'''','null::text')||',
                  p_load_type := '||coalesce(''''||p_load_type||'''','null::text')||',
                  p_delta_mode := '||coalesce(''''||p_delta_mode||'''','null::text')||',
                  p_load_interval := '||coalesce(''''||p_load_interval||'''::interval','null::interval')||',
                  p_start_load := '||coalesce(''''||p_start_load||'''::timestamp','null::timestamp')||',
                  p_end_load := '||coalesce(''''||p_end_load||'''::timestamp','null::timestamp')||');';            
      execute v_sql into v_load_id;
      perform fw.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Load_id for object '||rec.object_id||' is: '||v_load_id, 
       p_location    := v_location); --log function call
  END LOOP; 
    perform fw.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Finish Generate load_id for load_group '||p_load_group, 
       p_location    := v_location); --log function call
   return true; 
  exception when others then 
     raise notice 'Function % finished with error: %',v_location,sqlerrm;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Function '||v_location||' finished with error: '||SQLERRM, 
        p_location    := v_location);
     return false;  
END;


$$
EXECUTE ON ANY;
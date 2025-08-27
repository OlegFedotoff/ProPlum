-- DROP FUNCTION fw.f_wait_adwh_load(bpchar, int4, int4, int4, bool);

CREATE OR REPLACE FUNCTION fw.f_wait_adwh_load(p_object_id bpchar, p_hour_stop int4 DEFAULT 10, p_repeat_interval int4 DEFAULT 60, p_repeat_count int4 DEFAULT 60, p_check_today bool DEFAULT true)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
declare
    v_location     text    := 'fw.f_wait_adwh_load';
    v_repeat_count integer := 0; --count of repeats
    p_hour_now     integer;
begin
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START checking ADWH is ready: ' || p_object_id, 
       p_location    := v_location);
    while not exists(
      SELECT 1 flag_hybris
             FROM fw.load_info fl
             where object_id = cast(p_object_id as int) 
             and load_status = 3 
            and
             case when p_check_today 
              then DATE_TRUNC('DAY',created_dttm) = current_date
             else true 
             end)
       loop
          perform pg_sleep(p_repeat_interval);--interval in seconds
          v_repeat_count = v_repeat_count + 1;
          perform fw.f_write_log(
             p_log_type    := 'SERVICE',
             p_log_message := 'CONTINUE checking BW group_type ready: ' || p_object_id || '. Step number: ' ||v_repeat_count::text, 
             p_location    := v_location);
          if v_repeat_count = p_repeat_count then
              PERFORM fw.f_write_log(
                 p_log_type    := 'ERROR', 
                 p_log_message := 'Number of steps checking ADWH reached the limit ' || p_repeat_count::text, 
                 p_location    := v_location);
              return false;
          end if;
          
          select extract(hour from current_time) into p_hour_now;

          if p_hour_now>p_hour_stop then
                 PERFORM fw.f_write_log(
                 p_log_type    := 'SERVICE', 
                 p_log_message := 'Time checking ADWH reached the limit ' || p_hour_stop::text, 
                 p_location    := v_location);
             return true;
          end if;
        end loop;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END ADWH group_type is ready: ' || p_object_id, 
       p_location    := v_location);
   return true;
END;



$$
EXECUTE ON ANY;
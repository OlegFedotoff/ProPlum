-- DROP FUNCTION fw.f_wait_kdw_load_x(text, int4, int4, bool);

CREATE OR REPLACE FUNCTION fw.f_wait_kdw_load_x(p_group_type text, p_repeat_interval int4 DEFAULT 60, p_repeat_count int4 DEFAULT 60, p_check_today bool DEFAULT true)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
declare
    v_location     text    := 'fw.f_wait_kdw_load';
    v_repeat_count integer := 0; --count of repeats
begin
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START checking object is ready: ' || p_group_type, 
       p_location    := v_location);
    while not exists(
       select 1
         from fw.ext_load_info_x li --get kdw load_info
         where li.group_type = p_group_type and
             case when p_check_today 
              then date_trunc('day',li.load_date) = current_date 
             else true 
             end)
        loop
          perform pg_sleep(p_repeat_interval);--interval in seconds
          v_repeat_count = v_repeat_count + 1;
          perform fw.f_write_log(
             p_log_type    := 'SERVICE',
             p_log_message := 'CONTINUE checking KDW group_type ready: ' || p_group_type || '. Step number: ' ||v_repeat_count::text, 
             p_location    := v_location);
          if v_repeat_count = p_repeat_count then
              PERFORM fw.f_write_log(
                 p_log_type    := 'ERROR', 
                 p_log_message := 'Number of steps reached the limit ' || p_repeat_count::text, 
                 p_location    := v_location);
              return false;
          end if;
        end loop;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END object is ready: ' || p_group_type, 
       p_location    := v_location);
   return true;
END;





$$
EXECUTE ON ANY;
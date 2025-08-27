-- DROP FUNCTION fw.f_wait_bw_load(bpchar, int4, int4, bool);

CREATE OR REPLACE FUNCTION fw.f_wait_bw_load(p_group_type bpchar, p_repeat_interval int4 DEFAULT 60, p_repeat_count int4 DEFAULT 60, p_check_today bool DEFAULT true)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
declare
    v_location     text    := 'fw.f_wait_bw_load';
    v_repeat_count integer := 0; --count of repeats
begin
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'START checking BW group_type is ready: ' || p_group_type, 
       p_location    := v_location);
    while not exists(
       select 1
         from fw.ext_cv_flg_loads fl --get Komus.BaseViews.KDWplusBW/CV_FLG_LOADS
         where fl.zgrouptp = p_group_type
         and 
          coalesce(fl.zflagpr,0) = 1
            and
             case when p_check_today 
              then to_date(fl.CALDAY, 'YYYYMMDD') =current_date
             else true 
             end)
       loop
          perform pg_sleep(p_repeat_interval);--interval in seconds
          v_repeat_count = v_repeat_count + 1;
          perform fw.f_write_log(
             p_log_type    := 'SERVICE',
             p_log_message := 'CONTINUE checking BW group_type ready: ' || p_group_type || '. Step number: ' ||v_repeat_count::text, 
             p_location    := v_location);
          if v_repeat_count = p_repeat_count then
              PERFORM fw.f_write_log(
                 p_log_type    := 'ERROR', 
                 p_log_message := 'Number of steps checking BW reached the limit ' || p_repeat_count::text, 
                 p_location    := v_location);
              return false;
          end if;
        end loop;
    perform fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END BW group_type is ready: ' || p_group_type, 
       p_location    := v_location);
   return true;
END;





$$
EXECUTE ON ANY;
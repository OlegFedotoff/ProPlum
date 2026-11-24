-- DROP FUNCTION fw.f_set_load_id_error(int8);

CREATE OR REPLACE FUNCTION fw.f_set_load_id_error(p_load_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Set error status (-1) to load_id via dblink; sets updated_dttm in UTC.
 * For reload load_id (job_name = 'RELOAD') removes its request from fw.object_reload;
 * cleanup errors are only logged and never affect the status*/
declare
    v_location        text := 'fw.f_set_load_id_error';
    c_error_status    int  := -1;
    c_reload_job_name text := 'RELOAD';
    v_server          text;
    v_sql             text;
    v_job_name        text;
begin
    v_server = fw.f_get_constant('c_log_fdw_server');
	v_sql = 'update fw.load_info set load_status = ' || c_error_status::text || ', updated_dttm = (now() AT TIME ZONE ''UTC'') where load_id = ' || p_load_id::text;
    perform dblink(v_server,v_sql);
    begin
        select job_name into v_job_name from fw.load_info where load_id = p_load_id;
        if v_job_name = c_reload_job_name then
            delete from fw.object_reload where load_id = p_load_id;
            perform fw.f_write_log(
               p_log_type    := 'SERVICE',
               p_log_message := 'Reload request removed for load_id = '||p_load_id,
               p_location    := v_location,
               p_load_id     := p_load_id);
        end if;
    exception when others then
        perform fw.f_write_log(
           p_log_type    := 'ERROR',
           p_log_message := 'Reload request cleanup failed for load_id = '||p_load_id||': '||SQLERRM,
           p_location    := v_location,
           p_load_id     := p_load_id);
    end;
    perform fw.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Set in error load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
end;



$$
EXECUTE ON ANY;

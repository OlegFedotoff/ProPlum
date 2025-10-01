-- DROP FUNCTION fw.f_set_load_id_in_process_direct(int8);

CREATE OR REPLACE FUNCTION fw.f_set_load_id_in_process_direct(p_load_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$


/*Set in process status to load_id (direct update without dblink)*/
declare
    v_location             text := 'fw.f_set_load_id_in_process_direct';
    c_in_process_status    int  := 2;
begin
    update fw.load_info
       set load_status = c_in_process_status,
           updated_dttm = current_timestamp
     where load_id = p_load_id;

    perform fw.f_write_log(
       p_log_type := 'SERVICE', 
        p_log_message := 'Set in process load_id = '||p_load_id, 
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
end;


$$
EXECUTE ON ANY;

GRANT EXECUTE ON FUNCTION fw.f_set_load_id_in_process_direct(int8) TO role_data_loader;



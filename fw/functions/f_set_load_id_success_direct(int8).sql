-- DROP FUNCTION fw.f_set_load_id_success_direct(int8);

CREATE OR REPLACE FUNCTION fw.f_set_load_id_success_direct(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$





/*Set success status to load_id (direct update without dblink)*/
declare
    v_location             text := 'fw.f_set_load_id_success_direct';
    c_success_status       int  := 3;
begin
    update fw.load_info
       set load_status = c_success_status,
           updated_dttm = current_timestamp
     where load_id = p_load_id;

    perform fw.f_write_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Successfully finished processing load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
    return true;
end;





$$
EXECUTE ON ANY;


GRANT EXECUTE ON FUNCTION fw.f_set_load_id_success_direct(int8) TO role_data_loader;
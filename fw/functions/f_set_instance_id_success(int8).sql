-- DROP FUNCTION fw.f_set_instance_id_success(int8);

CREATE OR REPLACE FUNCTION fw.f_set_instance_id_success(instance_id int8)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Set in process status to instance_id*/
declare
    c_success_status int  := 3;
    v_server       text;
    v_sql          text;
begin
    v_server = fw.f_get_constant('c_log_fdw_server');
	v_sql = 'update fw.chains_info set status = ' || c_success_status::text || ', chain_finish = '''||current_timestamp||''' where instance_id = ' || instance_id::text;
    perform dblink(v_server,v_sql);
    perform fw.f_write_chain_log(
       p_log_type := 'SERVICE', 
       p_log_message := 'Set in success instance_id = '||instance_id, 
       p_instance_id := instance_id); --log function call
end;


$$
EXECUTE ON ANY;
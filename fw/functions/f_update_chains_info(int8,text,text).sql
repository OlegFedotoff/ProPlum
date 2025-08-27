-- DROP FUNCTION fw.f_update_chains_info(int8, text, text);

CREATE OR REPLACE FUNCTION fw.f_update_chains_info(p_instance_id int8, p_field_name text, p_value text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2024*/
/*Function update field of chains_info with value */
DECLARE
  v_sql text; 
  v_datatype text;
  v_res text;
  v_server text;
BEGIN
  perform fw.f_write_chain_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START set fw.chains_info'||p_field_name||' = '||coalesce(p_value,'{empty}')||' for instance_id = '||p_instance_id, 
     p_instance_id := p_instance_id); --log function call
  v_server = fw.f_get_constant('c_log_fdw_server');
  v_sql = 'select data_type from information_schema.columns where table_schema ||''.''||table_name = ''fw.chains_info'' and column_name = '''||p_field_name||'''';
  execute v_sql into v_datatype;
  if v_datatype is null then
     PERFORM fw.f_write_chain_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'No field with name '||p_field_name||' in table fw.chains_info', 
        p_instance_id := p_instance_id);
     raise exception 'No field with name % in table fw.chains_info',p_field_name;
  end if;
  v_sql = 'UPDATE fw.chains_info set '||p_field_name||'='''||p_value||'''::'||v_datatype||' where instance_id = '||p_instance_id;
  perform fw.f_write_chain_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'UPDATE sql is: '||v_sql, 
   p_instance_id := p_instance_id); --log function call
  v_res := dblink(v_server,v_sql); 
  --execute v_sql;
  perform fw.f_write_chain_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END set fw.chains_info.'||p_field_name||' = '||p_value||' for instance_id = '||p_instance_id, 
   p_instance_id := p_instance_id); --log function call
  exception when others then 
     raise notice 'ERROR %, while set fw.chains_info.% = % for instance_id = %',sqlerrm,p_field_name,coalesce(p_value,'{empty}'),p_instance_id;
     PERFORM fw.f_write_chain_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Set fw.chains_info.'||p_field_name||' = '||p_value||' for instance_id = '||p_instance_id||' finished with error: '||SQLERRM, 
        p_instance_id     := p_instance_id);
END;

$$
EXECUTE ON ANY;
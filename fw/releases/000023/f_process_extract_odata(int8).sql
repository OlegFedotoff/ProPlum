-- DROP FUNCTION fw.f_process_extract_odata(int8);

CREATE OR REPLACE FUNCTION fw.f_process_extract_odata(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
		
    /*New function*/
DECLARE
    v_location            text := 'fw.f_process_extract_odata';
    v_extraction_type     text;
    v_extraction_to       timestamp;
    v_extraction_from     timestamp;
    v_tmp_table_name      text;
    v_ext_table_name      text;
    v_delta_field         text;
    v_error               text;
    v_sql                 text;
    v_where               text;
    v_res                 bool;
    v_cnt                 int8;
    v_cnt_prev			  int8;
    v_server              text;
BEGIN
	perform fw.f_write_log(p_log_type := 'SERVICE', 
       p_log_message := 'START extract data for load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
   	--set load_id for session
       select oqh.sql_query, oqh.table_to, oqh.rows_count from fw.v_odata_query_helper oqh where oqh.load_id = p_load_id
       into v_sql, v_tmp_table_name, v_cnt_prev;
--       for update; --Заблокировать строку 
       raise notice 'v_cnt_prev is %', v_cnt_prev;
       v_server = lower(fw.f_get_constant('c_log_fdw_server'));
       if v_cnt_prev < 0 or v_cnt_prev is null then 
       		raise 'Previous thread was fallen. p_load_id is: %', p_load_id;
       end if;
        v_cnt =  fw.f_insert_table_sql(
           p_table_to := v_tmp_table_name,
           p_sql      := v_sql); --load from ext to stage (delta) table
        if v_cnt is not null then
         v_res = true;
        else 
         v_res = false;
        end if;  
    if v_res is true then
      -- Log Success
      perform fw.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END extract data for load_id = '||p_load_id||', '||v_cnt||' rows extracted',
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      v_sql = 'update fw.odata_query_helper set rows_count = '||(v_cnt + v_cnt_prev)||' where load_id = ' || p_load_id::text;
      perform dblink(v_server,v_sql);
      return v_res;
    else 
      perform fw.f_set_load_id_error(p_load_id := p_load_id);
      -- Log errors
      perform fw.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END extract data for load_id = '||p_load_id||' finished with error', 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      v_sql = 'update fw.odata_query_helper set rows_count = -1 where load_id = ' || p_load_id::text;
      perform dblink(v_server,v_sql);
      perform fw.f_set_load_id_error(p_load_id := p_load_id);
      return false;
     end if;
    return false;
   
    exception when others then 
     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
     perform fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Extract data for load_id '||p_load_id||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);  
     raise 'Current thread was fallen. p_load_id is: %', p_load_id;
     return false;
END;



$$
EXECUTE ON ANY;

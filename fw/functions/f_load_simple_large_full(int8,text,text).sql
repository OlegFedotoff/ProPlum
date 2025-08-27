-- DROP FUNCTION fw.f_load_simple_large_full(int8, text, text);

CREATE OR REPLACE FUNCTION fw.f_load_simple_large_full(p_load_id int8, p_src_table text, p_trg_table text DEFAULT NULL::text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
	
	
/*Function starts load function */
DECLARE
  v_location      text := 'fw.f_load_simple_large_full';
  v_object_id     int8;
  --v_cnt_rows      int8;
  v_src_table     text;
  v_trg_table     text;
  v_table         text; 
  v_int_field     text;
  v_max_id_value  int8;
  v_columns       text;

  v_interval      int8 := 500000; -- количество строк на сегмент в запросе 
  --v_cnt_seg       int;
  v_cnt           int8;
  --v_begin_cycle   int := 1;
  --v_end_cycle     int;
  v_begin         int8;
  v_end           int8;
  --v_step          int8; 
  v_pxf           text; -- pxf connection server
 
  v_sql          text;
  v_sql_conn     text;
  v_conn_str     text; 
 
BEGIN
 -- function load data from source large table into target
 
 perform fw.f_set_session_param(
    p_param_name := 'fw.load_id', 
    p_param_value := p_load_id::text);
   select ob.object_id, ob.object_name, substr( ob.object_name, position('.' in ob.object_name) + 1, length(ob.object_name) ), 
        ob.delta_field, ob.delta_field_format, ob.connect_string  
   from fw.objects ob  inner join 
	    fw.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id, v_trg_table, v_table, v_int_field, v_max_id_value, v_pxf; -- get object_id, target table...

   -- Количество работающих сегментов 
   --select count(*) from gp_segment_configuration where role = 'p' and status = 'u' and content <> -1
   --into v_cnt_seg; 
  
  v_src_table  = fw.f_unify_name(p_name := p_src_table);
  v_trg_table  = coalesce(fw.f_unify_name(p_name := p_trg_table),v_trg_table);
 perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START simple large full load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
/* 
  PERFORM fw.f_wait_locks(
     p_table_name      := v_trg_table, 
     p_repeat_interval := 60,
     p_repeat_count    := 60); --wait for no locks on main table every 1 minute 60 times
*/   
 
 --============================================================================ 
  -- Lock target table
  EXECUTE 'LOCK TABLE '||v_trg_table||' in ACCESS EXCLUSIVE MODE';
  -- truncate target table
  perform fw.f_truncate_table(v_trg_table);
 
  --v_end_cycle = v_max_id_value / v_interval;
  --v_end_cycle = 2;
  v_begin = 1;
  v_end   = v_interval * ( (v_max_id_value / v_interval) + 20 );
  --v_end  = 611000000;
  --v_step = v_end;
 
  -- get columns from target table
  select string_agg('"'||c.column_name||'"'||' '||
   case 
	when data_type = 'time' or data_type = 'time without time zone' then 'timestamp' 
	when data_type = 'character' then coalesce(data_type||'('||character_maximum_length||')',data_type)
	when data_type = 'interval'  then 'text'
	else data_type 
   end,',' order by c.ordinal_position) from information_schema.columns c where c.table_schema||'.'||c.table_name = v_trg_table
   into v_columns;

  --while v_begin_cycle < v_end_cycle + 2 loop

	perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'v_begin = '||v_begin||' , v_end = '||v_end||' , v_interval = '||v_interval, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
     
    v_sql := 'DROP external TABLE if exists '  || v_src_table ||'';
    EXECUTE v_sql;
    v_conn_str = v_table || '?profile=jdbc&server=' || v_pxf || '&PARTITION_BY=' || v_int_field || ':int&RANGE=' || v_begin || ':' || v_end || '&INTERVAL=' || v_interval || '';
    RAISE notice 'v_conn_str: %',coalesce(v_conn_str,'empty');  
   
      v_sql_conn :=
      'LOCATION (''pxf://'||v_conn_str||''') ON ALL FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'' )
       ENCODING ''UTF8''';
  
    v_sql :=  ' CREATE EXTERNAL TABLE '  || v_src_table ||' ('|| v_columns || ') ' ||v_sql_conn;
    raise notice 'v_ext_t_name: [%], v_columns: [%], v_sql_conn: [%]',v_src_table,v_columns,v_sql_conn;
    raise notice 'v_sql with columns: %',v_sql;
   -- v_sql :=  ' CREATE EXTERNAL TABLE '  || v_ext_t_name ||' (like '  || v_full_table_name || ') ' ||v_sql_conn;
  
   perform fw.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'v_sql:{'||coalesce(v_sql,'empty')||'}', 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
    
   EXECUTE v_sql;

   v_sql := 'insert into '|| v_trg_table || ' select * from '|| v_src_table || ' where '|| v_int_field || ' >= '|| v_begin || ' and '|| v_int_field || ' <= '|| v_end || '';
   EXECUTE v_sql;

   --v_begin = v_begin + v_step;
   --v_end   = v_end   + v_step;
   --v_begin_cycle = v_begin_cycle + 1;
  
  --end loop;         
 
 --Analyze table  
   PERFORM fw.f_analyze_table(p_table_name := v_trg_table);

   EXECUTE 'select max('||v_int_field||'), count(*) from '||v_trg_table||'' into v_max_id_value, v_cnt;
  
   perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'v_cnt = '||v_cnt||' , v_int_field = '||v_int_field||' , v_trg_table = '||v_trg_table||' , v_max_id_value = '||v_max_id_value||' , v_object_id = '||v_object_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
   
   v_sql := 'UPDATE fw.objects set delta_field_format = ' || v_max_id_value ||' where object_id = ' || v_object_id ||'';
   EXECUTE v_sql;

--============================================================================
   
   perform fw.f_update_load_info( 
     p_load_id    := p_load_id,
     p_field_name := 'row_cnt',
     p_value      := v_cnt::text);-- update row_cnt in load_info
  perform fw.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END simple large full load from table '||p_src_table||' into table '||p_trg_table||' with load_id = '||p_load_id||', '||v_cnt||' rows inserted', 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
 return true;
 exception when others then 
  raise notice 'ERROR Load object with load_id = %: %',p_load_id,SQLERRM;
   PERFORM fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Load object with load_id = '||p_load_id||' finished with error: '||SQLERRM, 
     p_location    := v_location,
     p_load_id     := p_load_id);
    perform fw.f_set_load_id_error(p_load_id := p_load_id);  
   return false;
END;







$$
EXECUTE ON ANY;
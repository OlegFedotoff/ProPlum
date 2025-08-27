-- DROP FUNCTION fw.f_delete_table_sql(text, text, _text);

CREATE OR REPLACE FUNCTION fw.f_delete_table_sql(p_table_name text, p_sql text, p_merge_key _text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	

/*Function delete rows via sql from target table*/
DECLARE
    v_location      text := 'fw.f_delete_table_sql';
    v_sql           text;
    v_table_name text;
    v_cnt           int8;
    v_merge_key     text;
begin
	
--delete rows from source sql (p_sql) into target table (p_table_to_name) using "merge key" from object settings 

  --Log
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start delete from table ' || p_table_name ||' from sql '||p_sql,
     p_location    := v_location); --log function call
     
  v_table_name   = fw.f_unify_name(p_name := p_table_name);
 
  if p_merge_key is null then
    perform fw.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'ERROR while delete from table ' ||v_table_name||', merge key for object is null', 
      p_location    := v_location); --log function call
    raise notice 'Merge key is null';
    return null;
  end if;

  --Generate script for update
  v_sql = 'with delete_sql as ('||p_sql||') delete from '||v_table_name|| ' as trg'||
          ' USING delete_sql '||
          ' where 1=1 '||array_to_string(array(select replace('AND (trg.MERGE_KEY = delete_sql.MERGE_KEY) ','MERGE_KEY', unnest(p_merge_key))), ' ');

  perform fw.f_write_log(
     p_log_type    := 'DEBUG', 
     p_log_message := 'Delete sql v_sql = '||v_sql,
     p_location    := v_location); --log function call
  --execute delete query
  execute v_sql;
 
  GET DIAGNOSTICS v_cnt = ROW_COUNT;
  raise notice '% rows deleted from sql: % from %',v_cnt, v_sql,v_table_name;   

  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End delete from table '||v_table_name||' from sql: '||p_sql,
     p_location    := v_location); --log function call
  return v_cnt;
 exception when others then 
  raise notice 'ERROR delete from table % from sql: %, ERROR: %',v_table_name,p_sql,SQLERRM;
  PERFORM fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'delete from table '|| v_table_name||' via sql: '||p_sql||' finished with error'', ERROR: '||SQLERRM, 
     p_location    := v_location);
   return null;
 
END;




$$
EXECUTE ON ANY;
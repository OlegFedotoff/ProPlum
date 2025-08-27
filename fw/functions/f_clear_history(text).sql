-- DROP FUNCTION fw.f_clear_history(text);

CREATE OR REPLACE FUNCTION fw.f_clear_history(p_schema_name text)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
DECLARE
    rec          record;
    v_date_from  date;
    v_date_to    date;
    v_location   text := 'fw.f_clear_history';
   
/*Function clear history data in schema*/
BEGIN

  select clean_from, fw.f_get_data_clear(leave) from fw.d_clear_history where schemaname = p_schema_name
  into v_date_from, v_date_to;
 
        perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Start clear history data in schema ' || p_schema_name ||' range from '||v_date_from||' to '||v_date_to, 
        p_location    := v_location); --log function call
 
  for rec in         
	SELECT n.nspname AS schemaname, c.relname AS tablename, a.attname AS columnname
	FROM pg_namespace n, pg_class c, pg_attribute a, ( SELECT p.parrelid, p.parlevel, p.paratts[i.i] AS attnum, i.i
	FROM pg_partition p, generate_series(0, ( SELECT max(array_upper(pg_partition.paratts, 1)) AS max
	FROM pg_partition)) i(i) WHERE p.paratts[i.i] IS NOT NULL) p
	WHERE p.parrelid = c.oid AND c.relnamespace = n.oid AND p.attnum = a.attnum AND a.attrelid = c.oid
	and a.atttypid in (1082,1114) -- тип поля партиционирования date, timestamp соответственно
	and lower(n.nspname) = lower(p_schema_name) 
	and c.relname not in (select table_partition from fw.d_not_clear_tables where schemaname = p_schema_name)
	order by 1,2
 
  -- Drop partition cycle
   loop
      perform fw.f_drop_tab_partition_date_range(
       p_table_name := rec.schemaname||'.'||rec.tablename ,
       p_partition_start := v_date_from,
       p_partition_end  := v_date_to
       );
   end loop;  

       perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'End clear history data in schema ' || p_schema_name ||' range from '||v_date_from||' to '||v_date_to, 
        p_location    := v_location); --log function call
        
  return true;
 
 exception when others then 
     raise notice 'ERROR clear history data in schema%: %',p_schema_name,SQLERRM;
     return false;
END;
 


$$
EXECUTE ON ANY;
-- DROP FUNCTION fw.f_upsert_table_sql(text, text, int8, bool, bool);

CREATE OR REPLACE FUNCTION fw.f_upsert_table_sql(p_table_to_name text, p_sql text, p_load_id int8, p_delete_duplicates bool DEFAULT false, p_analyze bool DEFAULT true)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
		 	 	 	 	 	 	 	
/*Function merges sql to another table by delete insert*/
DECLARE
    v_location          text := 'fw.f_upsert_table_sql';
    v_table_from_name   text;
    v_table_to_name     text;
    v_cnt int8;
begin
	
--Upsert rows from source sql (p_sql) into target table (p_table_to_name) using "merge key" from object settings 
--1. Delete all rows from target table which exist in source sql
--2. Insert all rows from source sql into target table
  --Log
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start upsert table ' || p_table_to_name ||' from sql '||p_sql,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
     
  v_table_to_name   = fw.f_unify_name(p_name := p_table_to_name);
  --create temp table with target structure
  v_table_from_name = fw.f_create_tmp_table(
     p_table_name   := v_table_to_name,
     p_prefix_name  := 'temp_',
     p_is_temporary := true);
  --insert data into temp table from input sql
  v_cnt = fw.f_insert_table_sql(
     p_table_to := v_table_from_name,
     p_sql := p_sql);
  if v_cnt is null then
    raise notice 'Insert data into table % from sql % finished with error: %',v_table_from_name, p_sql,sqlerrm;
    PERFORM fw.f_write_log(
       p_log_type    := 'ERROR', 
       p_log_message := 'Insert data into table'||v_table_from_name||' from sql '||p_sql||' finished with error: '||SQLERRM, 
       p_location    := v_location);
   return null;
  end if;
 -- upsert data from source to target
  v_cnt = fw.f_upsert_table(
     p_table_from_name := v_table_from_name,
     p_table_to_name   := v_table_to_name,
     p_load_id         := p_load_id,
     p_delete_duplicates := p_delete_duplicates,
     p_analyze         := p_analyze);

  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End upsert table '||v_table_to_name||' from '||v_table_from_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_cnt;
END;




$$
EXECUTE ON ANY;
-- DROP FUNCTION fw.f_delete_rows_from_tables(int8, text, text);

CREATE OR REPLACE FUNCTION fw.f_delete_rows_from_tables(p_load_id int8, p_schema text, p_table_names text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
	
	/*Semenkova Ekaterina 
    * 2024*/
/*Function deleted rows from tables by stg_kdw.ext_adwh_deleted_row_from_tables*/
/*
 * p_schema - upper schema(s) delimeters ',' ; if no - then null or '' - will select all schemas
 * p_table_names - upper schema.table(s) delimeters ',' ; if no - then null or '' - will selected all tables
*/

DECLARE
  v_location text := 'fw.f_delete_rows_from_tables';
  v_table_name  text;
  v_schema_name text;
  v_cnt         int8;
  v_cnt_del     int8:=0;
  v_cnt_ins     int8:=0;
  v_cnt1        int8;
  v_date        timestamp;
  v_tables_arr  text[];
  v_tables      text;
  v_schemas_arr text[];
  v_schemas     text;
  v_query       text;
  v_qyery_part  text;
  rec           record;
  v_object_id   int8;
  v_res         bool;
  
begin
	
 
   	perform fw.f_write_log(
		p_log_type    := 'INFO', 
		p_log_message := 'START ' || v_location || ' with load_id = ' || p_load_id , 
		p_location    := v_location,
		p_load_id     := p_load_id);
	
 select o.object_id 
 from fw.load_info li 
 inner join fw.objects o on li.object_id = o.object_id 
 where li.load_id = p_load_id 
 into v_object_id;	

raise notice 'v_object_id %',v_object_id;

v_qyery_part:='';
-- если указана схема
 if p_schema is not null and p_schema!='' then
    v_schemas :=p_schema;
    v_schemas_arr  :=string_to_array(v_schemas, ',');
   
   v_qyery_part:='';
   for i in array_lower(v_schemas_arr, 1)..array_upper(v_schemas_arr, 1) 
    loop
	    -- сформируем фрагмент запроса, проверяющий название схемы
	    if v_qyery_part='' then
	       v_qyery_part:='and schema_name in ('''||v_schemas_arr[i]||'''';
	    else
	    v_qyery_part:=v_qyery_part||', '''||v_schemas_arr[i]||'''';
    	end if;
    end loop;
    if v_qyery_part!='' then
	       v_qyery_part:=v_qyery_part||')';
	end if;
    raise notice 'v_schemas % query WHERE add: %',v_schemas, v_qyery_part;
    
 end if;  
 
  -- получим перечень таблиц в которых надо удалить записи, если наименования таблиц не заданы при вызове функции
 if p_table_names is null or p_table_names='' then	
  
 select  string_agg(schema_name||'.'||table_name, ','  ORDER BY schema_name, table_name)
  from (select distinct schema_name, table_name  from stg_kdw.ext_adwh_deleted_rows_from_tables_R
        where flag_del='N') t
  into v_tables;
 
 
 -- Если перечень таблиц пустой, заканчиваем выполнение функции
   if v_tables is null then
      perform etl.f_write_log(
        p_log_type    := 'INFO', 
        p_log_message := 'No tables for delete rows', 
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
    return true;
   end if;
 
    v_tables_arr  :=string_to_array(v_tables, ',');
  else
    v_tables :=p_table_names;
    v_tables_arr  :=string_to_array(v_tables, ',');
end if;

-- Выбираем строки для удаления, пройдя по массиву названий таблиц
for i in array_lower(v_tables_arr, 1)..array_upper(v_tables_arr, 1) 
    loop
	    v_table_name:=v_tables_arr[i];
	   
	    -- количество записей которое надо удалить из таблицы
	  v_query:='SELECT count(*) FROM stg_kdw.ext_adwh_deleted_rows_from_tables_R where schema_name||''.''||table_name='''||v_tables_arr[i]||''' and flag_del=''N'' '||v_qyery_part; 
	  raise notice 'v_query: %',v_query; 
	  EXECUTE v_query into v_cnt;

      PERFORM fw.f_write_log(
                    p_log_type    := 'INFO',  
                    p_log_message := 'Select '||v_cnt||'  rows for delete from table '||v_table_name,
                    p_location    := v_location,
                    p_load_id     := p_load_id);
	  raise notice 'Select % - count % rows for delete', v_table_name, v_cnt;
	   	   
	  v_cnt_del :=0;
	  v_cnt_ins :=0;
	 
	  v_query:='SELECT distinct * FROM stg_kdw.ext_adwh_deleted_rows_from_tables_R where schema_name||''.''||table_name='''||v_tables_arr[i]||''' and flag_del=''N'' '||v_qyery_part; 
	  
	 
	    if v_cnt>0 then
        FOR rec IN execute v_query
            loop
	         
	         -- удаление записи из таблицы
	         v_query:='DELETE FROM '||rec.schema_name||'.'||rec.table_name||' WHERE '||rec.key_values;
	         EXECUTE v_query;

	         GET DIAGNOSTICS v_cnt1 = ROW_COUNT;
	         v_cnt_del :=v_cnt_del+ v_cnt1;

	         -- Проставление флага удаления в KDW
	         v_query:='INSERT INTO stg_kdw.ext_adwh_deleted_rows_from_tables_W VALUES('''||rec.schema_name||''', '''||rec.table_name||''', '''||replace(rec.key_values, '''', '''''')||''', ''Y'', '''||rec.date_created||''', current_date)'; 
	  	     EXECUTE v_query;
	        
	  	     GET DIAGNOSTICS v_cnt1 = ROW_COUNT;
	  	     v_cnt_ins :=v_cnt_ins+ v_cnt1 ;
 
             END LOOP;
             PERFORM fw.f_write_log(
                      p_log_type    := 'INFO',  
                      p_log_message := v_cnt_del||' rows deleted from table '||v_table_name||'; '||v_cnt_ins||' flag_del set.',
                      p_location    := v_location,
                      p_load_id     := p_load_id);
                   
          else
          PERFORM fw.f_write_log(
                    p_log_type    := 'INFO',  
                    p_log_message := 'No rows for delete from table '||v_table_name,
                    p_location    := v_location,
                    p_load_id     := p_load_id);
          return true;         
          end if;         
    END LOOP;       

    v_res=true;
    return v_res;
END;





$$
EXECUTE ON ANY;
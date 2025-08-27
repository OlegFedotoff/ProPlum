-- DROP FUNCTION fw.f_load_simple(int8, text, text, bool);

CREATE OR REPLACE FUNCTION fw.f_load_simple(p_load_id int8, p_src_table text, p_trg_table text DEFAULT NULL::text, p_delete_duplicates bool DEFAULT false)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
			
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function starts simple load function */
DECLARE
  v_location  text := 'fw.f_load_simple';
  v_object_id int8;
  v_cnt       int8;
  v_src_table text;
  v_trg_table text;
  v_tmp_table text;
  v_buf_table text;
  v_schema    text;
  v_extraction_type text;
  v_extraction_to   timestamp;
  v_extraction_from timestamp;
  v_load_type text;
  v_res       bool;
  v_end_date  timestamp;
  v_delta_fld text;
  v_date_field text;
  v_bdate_fld text;
  v_where     text;
  v_extr_sql  text;
  v_merge_key _text;
BEGIN
 -- function load upsert data from source table into target
 perform fw.f_set_session_param(
    p_param_name := 'fw.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_id, ob.object_name, li.extraction_type, li.load_type,li.load_to,ob.delta_field,
        ob.bdate_field,li.extraction_from, li.extraction_to,
        case coalesce(li.extraction_type, ob.extraction_type) 
          when 'DELTA' then ob.delta_field
          when 'PARTITION' then ob.bdate_field
          else coalesce(ob.delta_field,ob.bdate_field,null)::text
        end
   from fw.objects ob  inner join 
	    fw.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id, v_trg_table, v_extraction_type, v_load_type, v_end_date,v_delta_fld,v_bdate_fld,v_extraction_from, v_extraction_to,v_date_field; -- get object_id, target table, load_type
  v_src_table  = fw.f_unify_name(p_name := p_src_table);
  v_trg_table  = coalesce(fw.f_unify_name(p_name := p_trg_table),v_trg_table);
  v_schema     = fw.f_get_table_schema(p_table := v_trg_table);
  v_tmp_table  = fw.f_create_tmp_table(
    p_table_name  := v_trg_table, 
    p_schema_name := 'stg_'||v_schema,
    p_prefix_name := 'tmp_', 
    p_drop_table  := true,
    p_is_temporary := false);
 perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START simple load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id ||', extraction_type = '||coalesce(v_extraction_type,'{empty}')||', load_type = '||coalesce(v_load_type,'{empty}'), 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_extr_sql = fw.f_get_extr_expression(
    p_load_id      := p_load_id, 
    p_source_table := v_src_table,
    p_trg_table    := v_tmp_table);
  --extract data from source into stage
   --v_where := fw.f_get_extract_where_cond(p_load_id := p_load_id);
   --v_extr_sql = v_extr_sql||' where '||v_where;

  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Extraction from  '||v_src_table||' into table '||v_tmp_table||' with sql: '||v_extr_sql, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_cnt = fw.f_insert_table_sql(
     p_table_to := v_tmp_table, 
     p_sql      := v_extr_sql, 
     p_truncate_tgt := true);
  if v_cnt is null then 
    raise notice 'ERROR Load object with load_id = %',p_load_id;
    PERFORM fw.f_write_log(
       p_log_type    := 'ERROR', 
       p_log_message := 'Load object with load_id = '||p_load_id||' finished with error', 
       p_location    := v_location,
       p_load_id     := p_load_id);
    perform fw.f_set_load_id_error(p_load_id := p_load_id);  
    return false;
  end if;
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Extraction from  '||v_src_table||' into table '||v_tmp_table||', '|| v_cnt||' rows extracted', 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  if v_cnt is not null then
     v_res = true;
     if v_cnt = 0 then -- in case of empty delta
        perform fw.f_update_load_info(
          p_load_id    := p_load_id, 
          p_field_name := 'extraction_to', 
          p_value      := v_extraction_from::text);
     else
        perform fw.f_update_load_info(
          p_load_id    := p_load_id, 
          p_field_name := 'extraction_to', 
          p_value      := coalesce(fw.f_get_max_value(v_tmp_table,v_date_field),v_extraction_to::text));
     end if;
  else 
     v_res = false;
  end if;
  --load data from stage into target
  --v_where = coalesce(fw.f_get_where_clause(p_object_id := v_object_id),'1=1');
  v_where = '1=1'; -- where condition applied to extraction, load all data from stage
  case 
   when v_load_type = 'FULL' then 
     v_res = fw.f_load_simple_full(
        p_load_id   := p_load_id, 
        p_src_table := v_tmp_table,
        p_trg_table := v_trg_table);
   when v_load_type = 'LARGE_FULL' then
     v_where = fw.f_get_where_cond(p_load_id := p_load_id, p_table_alias := v_src_table);
     v_res = fw.f_load_simple_large_full(
        p_load_id   := p_load_id,
        p_src_table := v_src_table,
        p_trg_table := v_trg_table);
   when v_load_type = 'DELTA_UPSERT' then 
     v_res = fw.f_load_simple_upsert(
        p_load_id   := p_load_id, 
        p_src_table := v_tmp_table,
        p_trg_table := v_trg_table,
        p_delete_duplicates := p_delete_duplicates,
        p_where     := v_where);
     v_end_date = least(coalesce(fw.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
   when v_load_type = 'DELTA_UPDATE' then 
     v_res = fw.f_load_simple_update(
        p_load_id   := p_load_id, 
        p_src_table := v_tmp_table,
        p_trg_table := v_trg_table,
        p_delete_duplicates := p_delete_duplicates,
        p_where     := v_where);
     v_end_date = least(coalesce(fw.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
   when v_load_type = 'PARTITION' then 
     v_cnt = fw.f_load_delta_partitions(
        p_load_id         := p_load_id, 
        p_table_from_name := v_tmp_table,
        p_table_to_name   := v_trg_table,
        p_merge_partitions:= false,
        p_where           := v_where,
        p_delete_duplicates := p_delete_duplicates);
     if v_cnt is null then
       v_res = false;
       perform fw.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     v_end_date = least(coalesce(fw.f_get_max_value(v_tmp_table,v_bdate_fld)::timestamp,v_end_date),v_end_date);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     v_res = true;
   when v_load_type = 'DELTA_MERGE' then 
     v_merge_key = fw.f_get_merge_key(p_object_id := v_object_id);
     v_buf_table = fw.f_create_tmp_table(
       p_table_name  := v_trg_table, 
       p_schema_name := fw.f_get_constant('c_stg_table_schema')||fw.f_get_table_schema(v_trg_table), 
       p_prefix_name := 'buf_',
       p_drop_table := true);
     v_cnt = fw.f_merge_tables(
        p_table_from_name := v_tmp_table,
        p_table_to_name   := v_trg_table, 
        p_where     := v_where, 
        p_merge_key := v_merge_key, 
        p_trg_table := v_buf_table);
     perform fw.f_switch_def_partition(
        p_table_from_name := v_buf_table,
        --p_table_to_name := v_tmp_table);
        p_table_to_name := v_trg_table);       
     v_end_date = least(coalesce(fw.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     if v_cnt is null then
       v_res = false;
       perform fw.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     PERFORM fw.f_analyze_table(p_table_name := v_trg_table);
     v_res = true;
   when v_load_type = 'DELTA' then 
     v_cnt = fw.f_insert_table(
        p_table_from := v_tmp_table,
        p_table_to   := v_trg_table,
        p_where      := v_where); --Insert data from stg to target table
     if v_cnt is null then
       v_res = false;
       perform fw.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     --Analyze table
     PERFORM fw.f_analyze_table(p_table_name := v_trg_table);
     v_end_date = least(coalesce(fw.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     v_res = true;
   when v_load_type = 'UPDATE_PARTITION' then 
     v_cnt = fw.f_load_delta_partitions(
        p_load_id         := p_load_id, 
        p_table_from_name := v_tmp_table,
        p_table_to_name   := v_trg_table,
        p_merge_partitions:= true,
        p_where           := v_where,
        p_delete_duplicates := p_delete_duplicates);
     if v_cnt is null then
       v_res = false;
       perform fw.f_set_load_id_error(p_load_id := p_load_id);  
       return false;
     end if;
     v_end_date = least(coalesce(fw.f_get_max_value(v_tmp_table,v_delta_fld)::timestamp,v_end_date),v_end_date);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'load_to',
        p_value      := v_end_date::text);
     perform fw.f_update_load_info(
        p_load_id    := p_load_id,
        p_field_name := 'row_cnt',
        p_value      := v_cnt::text);
     v_res = true;
   else
     perform fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'ERROR no such load_type '||v_load_type, 
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
     v_res = false;
     perform fw.f_set_load_id_error(p_load_id := p_load_id);  
   end case;
 perform fw.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END simple load from table '||v_src_table||' into table '||v_trg_table||' with load_id = '||p_load_id, 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
   if v_res is true then 
    perform fw.f_set_load_id_success(p_load_id := p_load_id);  
   else
    perform fw.f_set_load_id_error(p_load_id := p_load_id); 
   end if;
 return v_res;
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
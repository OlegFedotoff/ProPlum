-- DROP FUNCTION fw.f_close_rows_in_current_part(int8, text, text);

CREATE OR REPLACE FUNCTION fw.f_close_rows_in_current_part(p_load_id int8, p_table_to_name text, p_table_from_name text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
/*Function select from current partition only rows which no equal rows in load_table by merge_key with different date_to */
DECLARE
    v_location          text := 'fw.f_load_delta_update_partitions';
    v_table_from_name   text;
    v_table_to_name     text;
    v_table_to_name_current text;
    v_table_from_name_tmp   text;
    v_col_part          text;
    v_object_id         int8;
    v_start_bdate       timestamp;
    v_partitionrangestart text;
    v_end_bdate         timestamp;
    v_merge_key_arr     text[];
    v_merge_key         text;
    v_table_cols        text;
    v_sql               text;
    v_partition_key     text;
    v_prt_table         text;
    v_buf_table         text;
    v_where             text;
    v_where_cond        text;
    v_schema_name       text;
    v_schema_name_trg   text;
    v_cnt_prt           int8;
    v_cnt               int8;
    v_bdate_fld_type    text;
    v_query             text;
    rec                 record;
    v_merge_sql         text;
   v_res                bool;
begin

perform fw.f_set_session_param(
    p_param_name := 'gp_workfile_limit_per_query', 
    p_param_value := '35000000kB');

perform fw.f_set_session_param(
    p_param_name := 'gp_workfile_compression', 
    p_param_value := 'on');

  v_table_from_name   = fw.f_unify_name(p_name := p_table_from_name);
  v_table_to_name     = fw.f_unify_name(p_name := p_table_to_name); 
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start close rows in ' || v_table_to_name ||' using '||v_table_from_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  v_cnt = 0;
 --find object settings
  select o.object_id 
    from fw.load_info li 
     inner join fw.objects o on li.object_id = o.object_id 
    where li.load_id = p_load_id 
    into v_object_id;
  v_schema_name_trg = fw.f_get_table_schema(p_table := v_table_to_name);  -- target table schema name
  v_schema_name = 'stg_'||replace(replace(v_schema_name_trg,'src_',''),'stg_','');-- delta table schema name
  -- указать в fw.objects merge_key по которому будет проверяться закрытая запись -- например
  v_merge_key_arr   = fw.f_get_merge_key(v_object_id);
  if v_merge_key_arr is null then
    perform etl.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'ERROR while close rows in table ' || p_table_to_name ||' using '||p_table_from_name||', merge key for object is null', 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    raise exception 'Merge key for object % is null',v_object_id;
    v_res=false;
    return v_res;
  end if;
 v_merge_key := array_to_string(v_merge_key_arr,',');
 
  -- get target table columns
  SELECT string_agg(column_name, ','  ORDER BY ordinal_position) 
    INTO v_table_cols
   FROM information_schema.columns
   WHERE table_schema||'.'||table_name  = v_table_to_name;
  -- get current_partition
   select v_schema_name_trg||'.'||partitiontablename, partitionrangestart 
    into v_table_to_name_current, v_partitionrangestart 
    from pg_partitions p
  where p.schemaname||'.'||p.tablename = lower(v_table_to_name) and lower(p.partitionname) is not distinct from 'p_current';
 
 raise notice 'lower(v_table_to_name) = %  v_schema_name_trg=%', v_table_to_name, v_schema_name_trg;
 raise notice 'v_table_to_name_current = %  v_partitionrangestart=%', v_table_to_name_current, v_partitionrangestart;
 if v_table_to_name_current is null then
    perform etl.f_write_log(
      p_log_type    := 'ERROR', 
      p_log_message := 'ERROR while close rows in table ' || p_table_to_name ||' using '||p_table_from_name||', no current partition', 
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    raise exception 'Current partition for object % is null',v_object_id;
    return false;
  end if;
  
  -- get partition column name
   select columnname into v_col_part from  pg_catalog.pg_partition_columns p where p.schemaname||'.'||p.tablename = lower(v_table_to_name) ;
  -- start date for current partition 
  v_query=' select '||v_partitionrangestart;
         execute v_query into v_start_bdate;
        
     
  -- select count from current partition before close rows
  v_query='select count(1) from '||v_table_to_name_current;
         execute v_query into   v_cnt_prt;
  
  v_cnt = 0;
      --create temp_table from load_table with closed rows
      v_table_from_name_tmp=v_table_from_name||'_tmp';
      v_query='create table '||v_table_from_name_tmp||' as select * from '||v_table_from_name||' where '||v_col_part||'<'||v_partitionrangestart; 
      execute v_query;
      GET DIAGNOSTICS v_cnt = ROW_COUNT;
      raise notice 'Inserted into % - % rows', v_table_from_name_tmp, v_cnt;   
     
  v_cnt = 0;   
      --create buffer table;
    v_buf_table = fw.f_create_tmp_table(
        p_table_name  := v_table_to_name, 
        p_schema_name := v_schema_name,
        p_prefix_name := 'buf_', 
        p_suffix_name := '_'||to_char(v_start_bdate,'YYYYMMDD'),
        p_drop_table  := true);
    -- where clause for partition

    PERFORM fw.f_write_log(
       p_log_type    := 'DEBUG',
       p_log_message := 'v_merge_key:{'||v_merge_key||'}', 
       p_location    := v_location);
    -- Create merge statement
    v_merge_sql :=
    'INSERT INTO '||v_buf_table||'
    SELECT '||v_table_cols||'
    FROM (SELECT t2.'||v_col_part||' as '||v_col_part||'_t2, t1.* 
          FROM '||v_table_to_name_current||' t1
          LEFT JOIN '||v_table_from_name_tmp||' t2
          ON 1=1 '||array_to_string(array(select replace( 'AND (t1.MERGE_KEY IS NOT DISTINCT FROM t2.MERGE_KEY) ' 
		  , 'MERGE_KEY', unnest(v_merge_key_arr))), ' ')||'AND t2.'||v_col_part||'< t1.'||v_col_part||' 
          ) t WHERE '||v_col_part||'_t2 is null ';
   
   
    PERFORM fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'Begin Insert from '||v_table_from_name||' to '||v_table_to_name||'('||v_merge_sql||')', 
       p_location    := v_location);
    execute v_merge_sql;
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    -- Log
    PERFORM fw.f_write_log(
       p_log_type    := 'SERVICE', 
       p_log_message := 'END Insert data from '||v_table_from_name||' to '||v_table_to_name||', result table: '||v_buf_table||' - '||v_cnt::text||' rows', 
       p_location    := v_location);
     
   

       -- switch partition in target table
       perform fw.f_switch_partition(
             p_table_name        := v_table_to_name,
             p_partition_value   := v_start_bdate, -- current partition
             p_switch_table_name := v_buf_table);
       
       PERFORM fw.f_write_log(
          p_log_type    := 'SERVICE', 
          p_log_message := 'Drop table: '||v_buf_table, 
          p_location    := v_location,
          p_load_id     := p_load_id); --log function call
       execute 'drop table '||v_buf_table;
      
       PERFORM fw.f_write_log(
          p_log_type    := 'SERVICE', 
          p_log_message := 'Drop table: '||v_table_from_name_tmp, 
          p_location    := v_location,
          p_load_id     := p_load_id);
       execute 'drop table '||v_table_from_name_tmp;

  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End close rows in '|| p_table_to_name ||' using '||p_table_from_name||' closed '||v_cnt_prt-v_cnt||' rows', 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
    v_res=true;
    return v_res;
	
	END;




$$
EXECUTE ON ANY;
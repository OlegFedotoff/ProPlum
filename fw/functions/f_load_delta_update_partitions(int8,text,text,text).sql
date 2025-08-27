-- DROP FUNCTION fw.f_load_delta_update_partitions(int8, text, text, text);

CREATE OR REPLACE FUNCTION fw.f_load_delta_update_partitions(p_load_id int8, p_table_from_name text, p_table_to_name text, p_where text DEFAULT NULL::text)
	RETURNS int8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Load rows from source table (p_table_from_name) into target table (p_table_to_name) iteratively for partition interval */
DECLARE
    v_location          text := 'fw.f_load_delta_update_partitions';
    v_table_from_name   text;
    v_table_to_name     text;
    v_object_id         int8;
    v_start_bdate       timestamp;
    v_end_bdate         timestamp;
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
    v_merge_key         _text;
    rec                 record;
begin
  v_table_from_name   = fw.f_unify_name(p_name := p_table_from_name);
  v_table_to_name     = fw.f_unify_name(p_name := p_table_to_name); 
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'Start load partitions for ' || v_table_to_name ||' from '||v_table_from_name, 
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
  v_merge_key   = fw.f_get_merge_key(v_object_id);
  v_where_cond = '('||coalesce(p_where, '1=1')||')';

  --find partition key for table 
  v_partition_key = fw.f_get_partition_key(p_table_name := v_table_to_name);
  if v_partition_key is null then
   perform fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Load partitions for ' || p_table_to_name ||' - no partitions', 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
   raise exception 'ERROR Table %: has no partitions',v_table_to_name;
  end if;
  --get delta and bdate fields type
  select coalesce(data_type,'timestamp') from information_schema.columns c where c.table_schema||'.'||c.table_name = v_table_to_name and c.column_name = v_partition_key into v_bdate_fld_type;

  -- check if table has records
  v_sql = 'select count(1) from '||v_table_from_name ||' where '||v_where_cond;
  execute v_sql into v_cnt;
  if v_cnt = 0 then
   perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End load update partitions for ' || p_table_to_name ||' from '||p_table_from_name||', source table is empty, 0 records loaded', 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
   return 0;
  end if; 
  v_cnt = 0;
  -- find business date load interval
  v_sql = 'select min('||v_partition_key||')::'||v_bdate_fld_type||', max('||v_partition_key||')::'||v_bdate_fld_type||' from '||v_table_from_name|| ' where '|| v_where_cond;
  execute v_sql into v_start_bdate, v_end_bdate;
-- create partitions for new data 
  perform fw.f_create_date_partitions(
     p_table_name      := v_table_to_name, 
     p_partition_value := v_end_bdate);
-- loop over load interval
  for rec in (select * from fw.f_partition_name_list_by_date(p_table_name := v_table_to_name, p_partition_start := v_start_bdate, p_partition_end := v_end_bdate))
    loop
    raise notice 'Load interval: % - %',rec.partrangestart::timestamp,rec.partrangeend::timestamp;
    v_start_bdate = rec.partrangestart;
    v_end_bdate = rec.partrangeend; 
    PERFORM fw.f_write_log(
        p_log_type    := 'DEBUG', 
        p_log_message := 'v_start_bdate:{' || v_start_bdate || '}', 
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
    PERFORM fw.f_write_log(
        p_log_type    := 'DEBUG', 
        p_log_message := 'v_end_bdate:{' || v_end_bdate || '}', 
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
    v_prt_table = v_schema_name_trg||'.'||
        fw.f_partition_name_by_value(
           p_table_name      := v_table_to_name, 
           p_partition_value := v_start_bdate);
      --create buffer table;
    v_buf_table = fw.f_create_tmp_table(
        p_table_name  := v_table_to_name, 
        p_schema_name := v_schema_name,
        p_prefix_name := 'buf_', 
        p_suffix_name := '_'||to_char(v_start_bdate,'YYYYMMDD'),
        p_drop_table  := true);
    -- where clause for partition
     v_where = v_where_cond || ' and ('||v_partition_key||'>='''||v_start_bdate|| '''::timestamp and '||v_partition_key||'<'''||v_end_bdate||'''::timestamp'||')';
       PERFORM fw.f_write_log(
          p_log_type    := 'DEBUG', 
          p_log_message := 'v_where:{' || v_where || '}', 
          p_location    := v_location,
          p_load_id     := p_load_id); --log function call
       v_cnt_prt = fw.f_merge_tables(
          p_table_from_name := v_table_from_name, 
          p_table_to_name   := v_prt_table, 
          p_where           := v_where, 
          p_merge_key       := v_merge_key,
          p_trg_table       := v_buf_table);
       v_cnt = v_cnt + v_cnt_prt;
       if v_cnt_prt = 0 then 
          PERFORM fw.f_write_log(
             p_log_type    := 'SERVICE', 
             p_log_message := 'There are no new data for interval: '||v_start_bdate||' - '||v_end_bdate||', skip switch partition', 
             p_location    := v_location,
             p_load_id     := p_load_id); --log function call
       else 
       -- switch partition in target table
          perform fw.f_switch_partition(
             p_table_name        := v_table_to_name,
             p_partition_value   := v_start_bdate,
             p_switch_table_name := v_buf_table);
       end if;
       PERFORM fw.f_write_log(
          p_log_type    := 'SERVICE', 
          p_log_message := 'Drop table: '||v_buf_table, 
          p_location    := v_location,
          p_load_id     := p_load_id); --log function call
       execute 'drop table '||v_buf_table;
     END LOOP;
  perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'End load update partitions for '|| p_table_to_name ||' from '||p_table_from_name, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_cnt;
END;



$$
EXECUTE ON ANY;
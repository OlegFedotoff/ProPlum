-- DROP FUNCTION fw.f_union_partitions_to_month(int8);

CREATE OR REPLACE FUNCTION fw.f_union_partitions_to_month(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	


    /* Solovev D (nov 2024)
    * Komus
    * 2024*/
	/*preparing operations before loading*/
	DECLARE
	  v_location        text := 'fw.f_union_partitions_to_month';
	  v_load_type       text;
	  v_object_id       int8;
	  v_extraction_type text;
	  v_table_name      text;
	  v_full_table_name text;
	  v_load_method     text;
      v_load_interval   interval;
	  v_tmp_schema_name text;
	  v_error           text;
	  v_schema_name     text;
      v_schema_name_origin text;
	  v_tmp_prefix      text;
	  v_tmp_suffix      text := '';
	  /*v_ext_prefix      text;*/
	  v_ext_suffix      text := '';
      v_part_name       text;
      v_extraction_from date;
	  v_extraction_to   date;
      v_counter         date;
	  v_res             bool;
      v_repartitioning  bool;
      rec               record;
      rec1              record;
      v_date_start      date;
      v_date_end        date;
      v_buffer_table    text;
      v_sql             text;
      v_cal_date        date;
      v_cal_date_end    date;
      v_distribution_key text;
      v_partition_key   text;
      v_flag            boolean;
      v_cnt             numeric;
      v_cnt_origin       numeric;
      v_new_partition_name text;
      v_check           boolean;
 	BEGIN
    /*created from hdset 2025-03-13*/


    RAISE NOTICE 'Получаю информацию о load_id';

    -- Get table load type
    select ob.object_id, ob.object_name,
    coalesce(li.extraction_type, ob.extraction_type),
    coalesce(li.load_type, ob.load_type),
    coalesce(li.load_method, ob.load_method),
    ob.load_interval, (date_trunc('month', (li.extraction_from::date-interval '1 day')::date ) + interval '1 month')::date,


    least(date_trunc('month', (li.extraction_to + interval '1' day)::date) - interval '1' day,
    	  date_trunc('month', (current_date - period_of_physical_live_in_month * INTERVAL '1 month')::date) - interval '1' day
    		)
    , coalesce(oa.repartitioning, false) as repartitioning
    , oa.tmp_schema
    into v_object_id, v_full_table_name, v_extraction_type ,v_load_type, v_load_method,
         v_load_interval, v_extraction_from, v_extraction_to, v_repartitioning, v_tmp_schema_name
    from   fw.load_info li
    join   fw.objects ob on(ob.object_Id = li.object_Id)
    left join fw.objects_archivation oa on li.object_id = oa.object_id
    where  li.load_id =  p_load_id/*124999*/;

    RAISE NOTICE 'Получена информация о load_id';

    --Если перепартицирование не нужно, то пропускаем дальнейшие обработки
    IF v_repartitioning = false then
        perform fw.f_write_log(
          p_log_type    := 'SERVICE',
          p_log_message := 'END No need to repartitioning table '||v_full_table_name||'.',
          p_location    := v_location,
          p_load_id     := p_load_id); --log function call
        return true;
    end if;
    perform fw.f_update_load_info(
               p_load_id    := p_load_id,
               p_field_name := 'extraction_to',
               p_value      := v_extraction_to::timestamp::text);

    v_full_table_name  = fw.f_unify_name(p_name := v_full_table_name); -- full table name
    v_schema_name = fw.f_get_table_schema(v_full_table_name);--left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
    v_schema_name_origin = fw.f_get_table_schema(v_full_table_name);
    v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));-- table name wo schema
    v_schema_name = replace(replace(replace(v_schema_name,'src_',''),'stg_',''),'load_','');
    /*v_tmp_schema_name = coalesce(fw.f_get_constant(
      p_constant_name := 'c_stg_table_schema'),
      'stg_')||v_schema_name;*/
   -- v_tmp_schema_name = 'competitor_prices';
    --Log
    perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'START Before union partitions to month-partition for table '||v_full_table_name,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

	--set load_id for session
    perform fw.f_set_session_param(
       p_param_name  := 'fw.load_id',
       p_param_value := p_load_id::text);
    perform fw.f_set_load_id_in_process(p_load_id := p_load_id);
    v_date_start = date_trunc('month', v_extraction_from)::date;
    v_date_end = (date_trunc('month', v_extraction_to)+interval '1 month' - interval '1 day')::date;
    --Checking that start date less then end date
    IF v_extraction_from >= v_extraction_to then
      v_error := 'Unable to process union partitions from ('||v_extraction_from||') to ('||v_extraction_to||') dates ';
      perform fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := 'Error while processing before job tasks: '||v_error,
        p_location    := v_location,
        p_load_id     := p_load_id); --log function call
      return false;
    END IF;
    RAISE NOTICE 'Union partitions in table % from % to %', v_full_table_name, v_date_start, v_date_end;
   /* perform fw.f_write_log(
                         p_log_type    := 'SERVICE',
                         p_log_message := 'INFO Union partitions in table '|| p_table_name ||' from '||v_date_start||' to '||v_date_end,
						 p_location    := v_location,
                         p_load_id     := p_load_id);*/
    v_distribution_key = fw.f_get_distribution_key(v_full_table_name);
    v_partition_key = fw.f_get_partition_key(v_full_table_name);
    v_cal_date = v_date_start;
    while v_cal_date < v_date_end loop
      v_buffer_table = v_tmp_schema_name||'.'||v_table_name||'_m_'||to_char(v_cal_date, 'MM_YYYY');--v_full_table_name||'_m_'||to_char(v_cal_date, 'MM_YYYY');
      v_sql = 'create table ' || v_buffer_table || ' WITH (appendonly=''true'', orientation=''column'', compresstype=zstd, compresslevel=''1'') as select * from '|| v_full_table_name ||' where 1=0 '||v_distribution_key;
      RAISE NOTICE 'v_buffer_table % v_cal_date % v_sql %', v_buffer_table, v_cal_date, v_sql;

      execute v_sql;
      execute 'ALTER TABLE ' || v_buffer_table || ' OWNER TO komus_devops';
      execute 'GRANT ALL ON TABLE ' || v_buffer_table || ' TO komus_devops';-- permissions
      v_flag = true;
      FOR rec IN
        select * from fw.f_partition_name_list_by_date(v_full_table_name, v_cal_date::timestamp, (v_cal_date+interval '1 month' - interval '1 day')::timestamp)
     /* select * from fw.f_partition_name_list_by_date('src_hybris.customertracking$2',
      '2024-07-07', '2024-07-07')*/

      LOOP
        if fw.f_check_tab_part_is_insertable_into(v_full_table_name, rec.partname) is not true then --Проверим, что рассматриваемая партиция все еще физическая = в нее можно записывать данные. Если в нее нельзя записывать данные, то она уже EXTERNAL и создавать ее повторно не нужно
          /*perform fw.f_write_log(
                         p_log_type    := 'SERVICE',
                         p_log_message := 'INFO Insert into buffer table for monthly union from '||rec.partname,
						 p_location    := v_location,
                         p_load_id     := p_load_id);*/

             v_flag = false;
             RAISE NOTICE 'Negative status of checking % is physical', rec.partname;
             v_error := 'Unable to process union partitions '||v_full_table_name||'when check count insertable into status of partition '||rec.partname;
             perform fw.f_write_log(
               p_log_type    := 'ERROR',
               p_log_message := 'Error while processing before job tasks: '||v_error,
               p_location    := v_location,
               p_load_id     := p_load_id); --log function call
             perform fw.f_set_load_id_error(p_load_id := p_load_id);
             return false;
        end if;
      END LOOP;
      v_cal_date_end = (v_cal_date + interval '1 month')::date;
      v_sql = 'select count(1) from '||v_full_table_name||' where '||v_partition_key||' >= '''||v_cal_date||''' and '||v_partition_key||'<'''||v_cal_date_end||'''';
      execute v_sql into v_cnt_origin;
      v_sql = 'select * from '||v_full_table_name||' where '||v_partition_key||' >= '''||v_cal_date||''' and '||v_partition_key||'<'''||v_cal_date_end||'''';
      v_cnt = null;
      v_cnt =  fw.f_insert_table_sql(p_table_to := v_buffer_table,
                                         p_sql := v_sql);
      RAISE NOTICE 'Inserted into v_buffer_table % from sql % rows: %', v_buffer_table, v_sql, v_cnt;
      if v_cnt <> v_cnt_origin then
        RAISE NOTICE 'Counts in origin partitions and union partition not equals!!! %', v_buffer_table;
        v_error := 'Counts in origin partitions and union partition not equals!!! %', v_buffer_table;
        perform fw.f_write_log(
          p_log_type    := 'ERROR',
          p_log_message := 'Error while processing before job tasks: '||v_error,
          p_location    := v_location,
          p_load_id     := p_load_id); --log function call
        perform fw.f_set_load_id_error(p_load_id := p_load_id);
        return false;
      elsif v_cnt = v_cnt_origin and v_cnt = 0 then
        RAISE NOTICE 'Count in partitions - 0 rows, skip union partitions for this month %', v_buffer_table;
        perform fw.f_write_log(
          p_log_type    := 'SERVICE',
          p_log_message := 'INFO Count in partitions - 0 rows, skip union partitions for this month '||v_buffer_table,
          p_location    := v_location,
          p_load_id     := p_load_id);
        perform fw.f_write_log(
            p_log_type    := 'SERVICE',
            p_log_message := 'INFO Start drop buffer table '||v_buffer_table,
            p_location    := v_location,
            p_load_id     := p_load_id);
        v_sql = 'drop table '|| v_buffer_table;
        EXECUTE v_sql;
      else
        perform fw.f_write_log(
          p_log_type    := 'SERVICE',
          p_log_message := 'INFO Start drop original partitions '||v_full_table_name,
          p_location    := v_location,
          p_load_id     := p_load_id);
        FOR rec1 IN
          select * from fw.f_partition_name_list_by_date(v_full_table_name, v_cal_date::timestamp, (v_cal_date+interval '1 month' - interval '1 day')::timestamp)
     /* select * from fw.f_partition_name_list_by_date('src_hybris.customertracking$2',
        '2024-07-07', '2024-07-07')*/
        LOOP
          v_sql = 'ALTER TABLE ' || v_full_table_name || ' drop partition if exists for ('''|| rec1.partrangestart||'''::timestamp)';
          RAISE NOTICE 'DROP PARTITION %', v_sql;
          execute v_sql;
        END LOOP;
        v_new_partition_name = 'm_'|| to_char(v_cal_date, 'MM_YYYY');
        perform fw.f_write_log(
          p_log_type    := 'SERVICE',
          p_log_message := 'INFO Creating monthly partition '||v_new_partition_name,
          p_location    := v_location,
          p_load_id     := p_load_id);
        v_sql = 'ALTER TABLE '||v_full_table_name||' SPLIT DEFAULT PARTITION START ('''||v_cal_date||'''::timestamp) END ('''||v_cal_date_end||'''::timestamp) INTO (PARTITION '|| v_new_partition_name ||', default partition)';
        RAISE NOTICE 'SPLIT DEFAULT PARTITION %', v_sql;
        execute v_sql;
        v_sql = 'alter table '||v_full_table_name||' exchange partition '||v_new_partition_name||' with table ' || v_buffer_table || ' with validation';
        RAISE NOTICE 'EXCHANGE PARTITION %', v_sql;
        EXECUTE v_sql;
        v_sql = 'select (count(1) = 0)::boolean from '||v_buffer_table;
        RAISE NOTICE 'CHECK COUNT %', v_sql;
        EXECUTE v_sql INTO v_check;
        if v_check then
          perform fw.f_write_log(
            p_log_type    := 'SERVICE',
            p_log_message := 'INFO Start drop buffer table '||v_buffer_table,
            p_location    := v_location,
            p_load_id     := p_load_id);
          v_sql = 'drop table '|| v_buffer_table;
          EXECUTE v_sql;
        else
          RAISE NOTICE 'ERROR when check count in buffer table %', v_buffer_table;
          v_error := 'Unable to process union partitions '||v_full_table_name||'when check count in buffer table '||v_buffer_table;
          perform fw.f_write_log(
            p_log_type    := 'ERROR',
            p_log_message := 'Error while processing before job tasks: '||v_error,
            p_location    := v_location,
            p_load_id     := p_load_id); --log function call
          perform fw.f_set_load_id_error(p_load_id := p_load_id);
          return false;
        end if;
      end if;
      v_cal_date = (v_cal_date + interval '1 month')::date;
    END LOOP;
     -- Log
    perform fw.f_write_log(
      p_log_type    := 'SERVICE',
      p_log_message := 'END Before job tasks processing for table '||v_full_table_name,
      p_location    := v_location,
      p_load_id     := p_load_id); --log function call
    perform fw.f_update_load_info(
             p_load_id    := p_load_id,
             p_field_name := 'extraction_from',
             p_value      := v_extraction_from::timestamp::text);
    perform fw.f_update_load_info(
             p_load_id    := p_load_id,
             p_field_name := 'extraction_to',
             p_value      := v_extraction_to::timestamp::text);
    return true;
 exception when others then
     raise notice 'ERROR while union partitions of table %: %',v_table_name,SQLERRM;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := 'Union partitions of table '||v_full_table_name||' finished with error: '||SQLERRM,
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);
     return false;
END;



$$
EXECUTE ON ANY;
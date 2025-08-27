-- DROP FUNCTION fw.f_archive_data(int8);

CREATE OR REPLACE FUNCTION fw.f_archive_data(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	



	    /*Solovev D
	    * KOMUS
	    * oct 2024*/
	/*Function archives data into s3*/
	DECLARE
	    v_location            text := 'fw.f_archive_data';
	    v_extraction_type     text;
	    v_extraction_to       date;
	    v_extraction_from     date;
	    v_tmp_table_name      text;
	    v_ext_table_name      text;
	    v_delta_field         text;
	    v_error               text;
	    v_sql                 text;
	    v_sql_order_by        text;
	    v_sql_compare_origin  text;
	    v_sql_compare_external text;
	    v_sql_drop_table      text;
	    v_sql_check           text;
	    v_where               text;
	    v_res                 bool;
	    v_cnt                 int8;
	    v_cnt_sum             int8;
	    v_cnt_flag            bool;
	    v_cnt_compare_origin  int8;
	    v_cnt_compare_external int8;
	    v_ext_table_base      text;
	    v_cur_ext_table       text;
	    v_cur_ext_table_write text;
	    v_object_name         text;
	    v_load_interval       interval;
	    v_load_method         text;
	    v_flag_not_empty      boolean;
	    v_tmp_schema_name     text;
	    v_full_table_name     text;
	    v_schema_name         text;
	    v_schema_name_origin  text;
	    rec                   record;
	    v_ext_read_part_name  text;
        v_delete_physical 	  bool;

	BEGIN
	    /*created  from hdset 2025-03-13*/
		perform fw.f_write_log(p_log_type := 'SERVICE',
	       p_log_message := 'START archive data to S3 for load_id = '||p_load_id,
	       p_location    := v_location,
	       p_load_id     := p_load_id); --log function call
	   	--set load_id for session
	    perform fw.f_set_session_param(
	       p_param_name  := 'fw.load_id',
	       p_param_value := p_load_id::text);
	    perform fw.f_set_load_id_in_process(p_load_id := p_load_id);
	    -- Get table load type
	    v_sql := 'select coalesce(li.extraction_type, ob.extraction_type),
	              case coalesce(li.extraction_type, ob.extraction_type)
	                when ''DELTA'' then ob.delta_field
	                when ''PARTITION'' then ob.bdate_field
	                else coalesce(ob.delta_field,ob.bdate_field,null)::text
	              end,
	              least(date_trunc(''month'', (li.extraction_to + interval ''1'' day)::date) - interval ''1'' day,
	    	      date_trunc(''month'', (current_date - period_of_physical_live_in_month * INTERVAL ''1 month'')::date) - interval ''1'' day
	    		  )::date,
	              li.extraction_from::date,
	              ob.object_name,
	              ob.load_interval,
	              ob.load_method,
	              ''stg_''||replace(replace(replace(fw.f_get_table_schema(ob.object_name),''src_'',''''),''stg_'',''''),''load_'','''')||''.''||right(ob.object_name,length(ob.object_name) - POSITION(''.'' in ob.object_name)),
	              coalesce(oa.sort_sentence, ''''),
	              coalesce(oa.tmp_schema),
                  oa.delete_physical
	              from fw.load_info li join fw.objects ob
	                on li.object_id = ob.object_id
	              left join fw.objects_archivation oa
	                on li.object_id = oa.object_id
	             where
	              li.load_id = ' ||
	             p_load_id::text;
	    execute v_sql into v_extraction_type, v_delta_field, v_extraction_to, v_extraction_from,
	    v_object_name, v_load_interval, v_load_method, v_ext_table_base, v_sql_order_by, v_tmp_schema_name, v_delete_physical;
	    perform fw.f_update_load_info(
	             p_load_id    := p_load_id,
	             p_field_name := 'extraction_to',
	             p_value      := v_extraction_to::timestamp::text);
	    v_full_table_name  = fw.f_unify_name(p_name := v_object_name); -- full table name
	    v_schema_name = fw.f_get_table_schema(v_full_table_name);--left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
	    v_schema_name_origin = fw.f_get_table_schema(v_full_table_name);
	    v_schema_name = replace(replace(replace(v_schema_name,'src_',''),'stg_',''),'load_','');
	    /*v_tmp_schema_name = coalesce(
	      fw.f_get_constant(
	      p_constant_name := 'c_stg_table_schema'),
	      'stg_')||v_schema_name;*/
	    --Checking that start date less then end date
	    IF v_load_method = 's3_archive' and v_extraction_from > v_extraction_to then
	     v_error := 'Unable to process archivation from ('||v_extraction_from||') to ('||v_extraction_to||') dates ';
	        perform fw.f_write_log(
	           p_log_type    := 'ERROR',
	           p_log_message := 'Error while processing before job tasks: '||v_error,
	           p_location    := v_location,
	           p_load_id     := p_load_id); --log function call
	        return false;
	    END IF;
	    -- process where clause
	    v_cnt_sum := 0;
	    v_res = true;
	    IF v_extraction_type in (select distinct extraction_type from fw.d_extraction_type) then
	      FOR rec IN
	        select partname, partrangestart, partrangeend from fw.f_partition_name_list_by_date(v_object_name, v_extraction_from, v_extraction_to)
	      LOOP
	        v_sql_check := 'SELECT EXISTS (SELECT 1 FROM '||v_schema_name_origin||'.'||rec.partname||' LIMIT 1)';
	        execute v_sql_check into v_flag_not_empty;
	        RAISE NOTICE 'v_flag_not_empty %', v_flag_not_empty;
	        perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO is partition '||rec.partname||' not empty? '||v_flag_not_empty,
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);
	        if fw.f_check_tab_part_is_insertable_into(v_object_name, rec.partname) is true and v_flag_not_empty is true then --Проверим, что рассматриваемая партиция все еще физическая = в нее можно записывать данные. Если в нее нельзя записывать данные, то она уже EXTERNAL и создавать ее повторно не нужно
	          v_sql := 'select * from '||v_schema_name_origin||'.'||rec.partname||' '||v_sql_order_by; --Попробовать сортировку
	          RAISE NOTICE 'Insert into external S3 table % from SQL: %', v_tmp_schema_name||'.'||rec.partname||'write', v_sql;
	          v_cnt = null;
	          v_cnt =  fw.f_insert_table_sql(p_table_to := v_tmp_schema_name||'.'||rec.partname||'write',
	                                         p_sql := v_sql);
	          if v_cnt is null then
	            v_res = false; -----------НУЖНО ВЫХОДИТЬ ИЗ ЦИКЛА. Или нет? Может быть пустая партиция
	            v_error := '0 rows when insert rows in f_insert_table_sql from sql: '||v_sql;
	            perform fw.f_write_log(
	              p_log_type := 'ERROR',
	              p_log_message := 'Error while extraction: ' || v_error,
	              p_location    := v_location,
	              p_load_id     := p_load_id); --log function call
	            RAISE NOTICE '0 rows to insert. No need to do external readable table and switch partitions with origin table. %',v_error;
	            perform fw.f_set_load_id_error(p_load_id := p_load_id);
	            return v_res;
	          else
	            v_cnt_sum = v_cnt_sum + v_cnt;
	            RAISE NOTICE 'Creating readable external table for switch partition % with % rows', rec.partname, v_cnt;
	            v_ext_read_part_name = null;
	            perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO start creating external readable table in s3 '||rec.partname||' with '||v_cnt||' rows.',
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);
	            select fw.f_create_ext_table(p_table_name := v_schema_name_origin||'.'||rec.partname/*'src_hybris.customertracking$2'*/,
	                                         p_load_id := p_load_id,
	                                         p_load_method := v_load_method,
	                                         p_schema_name := v_tmp_schema_name,
	                                         p_is_writeble_type := false)
	              into v_ext_read_part_name;

	            perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO start compare count in original partition '||v_schema_name_origin||'.'||rec.partname||' and external table '||v_ext_read_part_name,
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);
	            v_sql_compare_origin := 'select count(1) from '||v_schema_name_origin||'.'||rec.partname;
	            v_cnt_compare_origin := null;
	            execute v_sql_compare_origin into v_cnt_compare_origin;
	            v_sql_compare_external := 'select count(1) from '||v_ext_read_part_name;
	            v_cnt_compare_external := null;
	            execute v_sql_compare_external into v_cnt_compare_external;
				if v_cnt_compare_origin = v_cnt_compare_external then
	              perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO start switch partition in '||v_object_name||' with '||v_ext_read_part_name,
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);
	              RAISE NOTICE 'Switch partition % in %', v_ext_read_part_name, v_object_name;
	              PERFORM fw.f_switch_partition(
	                   p_table_name     := v_object_name,
	                   p_partition_name := rec.partname,
	                   p_switch_table_name := v_ext_read_part_name,
	                   p_external       := TRUE);

	              perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO start drop physical origin partition '||v_ext_read_part_name,
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);

                  if v_delete_physical then
                      RAISE NOTICE 'Start drop physical origin partition %', v_ext_read_part_name;
	                  v_sql_drop_table := 'DROP TABLE '||v_ext_read_part_name;
	                  execute v_sql_drop_table;
                  else
                      RAISE NOTICE 'Drop physical partition turned off for %', v_ext_read_part_name;
	              end if;

	              perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO start drop external writable table '||v_ext_read_part_name,
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);
	              RAISE NOTICE 'Start drop external table %', v_ext_read_part_name;
	              v_sql_drop_table := 'DROP EXTERNAL TABLE '||v_tmp_schema_name||'.'||rec.partname||'write';
	              execute v_sql_drop_table;
	            else
	              v_res = false;
	              v_error := 'Rows in origin partition and external tables not equal. In origin '||v_cnt_compare_origin||' in external table '||v_cnt_compare_external;
	              perform fw.f_write_log(
	                p_log_type := 'ERROR',
	                p_log_message := 'Error while extraction: ' || v_error,
	                p_location    := v_location,
	                p_load_id     := p_load_id); --log function call
	              RAISE NOTICE '0 rows to insert. No need to do external readable table and switch partitions with origin table. %',v_error;
	              perform fw.f_set_load_id_error(p_load_id := p_load_id);
	              return v_res;
	            end if;
	          end if;

	------------нужно дропнуть v_ext_read_part_name и внешнюю writable таблицу
	--дропать при условии, что в физической старой партиции такое же количество строк, как и во внешней таблице

	        else perform fw.f_write_log(
	                         p_log_type    := 'SERVICE',
	                         p_log_message := 'INFO partition '||rec.partname||' is already external or empty. Please check it. Param of NOT empty: '||v_flag_not_empty||' (true = not empty, false = empty)',
	                         p_location    := v_location,
	                         p_load_id     := p_load_id);
	        end if;
	      END LOOP;
	      if v_res is true then
	         if v_cnt_sum = 0 then -- in case of empty delta
	           perform fw.f_update_load_info(
	             p_load_id    := p_load_id,
	             p_field_name := 'row_cnt',
	             p_value      := v_cnt_sum::text);
	         else
	           perform fw.f_update_load_info(
	             p_load_id    := p_load_id,
	             p_field_name := 'row_cnt',
	             p_value      := v_cnt_sum::text);
	         end if;
	        else
	         v_res = false;
	        end if;
	     ELSE
	        v_error := 'Unable to process extraction type '||v_extraction_type;
	        perform fw.f_write_log(
	           p_log_type := 'ERROR',
	           p_log_message := 'Error while extraction: ' || v_error,
	           p_location    := v_location,
	           p_load_id     := p_load_id); --log function call
	        RAISE NOTICE '%',v_error;
	        perform fw.f_set_load_id_error(p_load_id := p_load_id);
	        v_res = false;
	    END IF;
	    if v_res is true then
	      -- Log Success
	      perform fw.f_write_log(
	         p_log_type := 'SERVICE',
	         p_log_message := 'END extract data for load_id = '||p_load_id||', '||v_cnt_sum||' rows extracted',
	         p_location    := v_location,
	         p_load_id     := p_load_id); --log function call

	      PERFORM fw.f_set_load_id_success(p_load_id := p_load_id);
	      return v_res;
	    else
	      PERFORM fw.f_set_load_id_error(p_load_id := p_load_id);
	      -- Log errors
	      perform fw.f_write_log(
	         p_log_type := 'SERVICE',
	         p_log_message := 'END extract data for load_id = '||p_load_id||' finished with error',
	         p_location    := v_location,
	         p_load_id     := p_load_id); --log function call
	      return false;
	     end if;
	    exception when others then
	     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
	     PERFORM fw.f_write_log(
	        p_log_type    := 'ERROR',
	        p_log_message := 'Extract data for load_id '||p_load_id||' finished with error: '||SQLERRM,
	        p_location    := v_location,
	        p_load_id     := p_load_id);
	     perform fw.f_set_load_id_error(p_load_id := p_load_id);
	     return false;
	END;



$$
EXECUTE ON ANY;
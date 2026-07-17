CREATE OR REPLACE FUNCTION fw.f_restore_archive_data(p_load_id bigint)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $$



	    /*dvs65
	    * KOMUS
	    * jul 2026*/
	/*Восстановление архивных (S3/external) партиций в физическое хранение ADWH.
	  Зеркало fw.f_archive_data:
	  - обрабатывает партиции в диапазоне load_info.extraction_from..extraction_to
	  - уже физические (insertable) партиции пропускает
	  - external-партиции восстанавливает: buffer INSERT из S3 + EXCHANGE
	  - delete_physical из objects_archivation: после EXCHANGE удаляет
	    вытесненный external-объект (симметрично удалению физической копии при архивации).
	  Важно: DROP EXTERNAL не удаляет parquet-файлы в бакете S3;
	  объекты в S3 остаются, пока их не удалят вне GP.*/
	DECLARE
	    v_location            text := 'fw.f_restore_archive_data';
	    v_extraction_type     text;
	    v_extraction_to       date;
	    v_extraction_from     date;
	    v_error               text;
	    v_sql                 text;
	    v_sql_check           text;
	    v_res                 bool;
	    v_cnt                 int8;
	    v_cnt_sum             int8;
	    v_cnt_compare_origin  int8;
	    v_cnt_compare_buffer  int8;
	    v_object_name         text;
	    v_load_method         text;
	    v_tmp_schema_name     text;
	    v_full_table_name     text;
	    v_schema_name_origin  text;
	    v_table_name          text;
	    rec                   record;
	    v_delete_physical     bool;
	    v_buffer_table        text;
	    v_distribution_key    text;
	    v_table_owner         text;
	    v_columns             text;
	    v_part_columns        text;
	    v_parent_schema_fp    text;
	    v_part_full_name      text;

	BEGIN
		perform fw.f_write_log(p_log_type := 'SERVICE',
	       p_log_message := 'START restore archive data from S3 for load_id = '||p_load_id,
	       p_location    := v_location,
	       p_load_id     := p_load_id);

	    perform fw.f_set_session_param(
	       p_param_name  := 'fw.load_id',
	       p_param_value := p_load_id::text);
	    perform fw.f_set_load_id_in_process(p_load_id := p_load_id);

	    -- period_of_physical_live_in_month не применяем: restore как раз для старых архивных периодов
	    select coalesce(li.extraction_type, ob.extraction_type),
	           li.extraction_to::date,
	           li.extraction_from::date,
	           ob.object_name,
	           coalesce(li.load_method, ob.load_method),
	           coalesce(oa.tmp_schema),
	           coalesce(oa.delete_physical, false)
	      into v_extraction_type,
	           v_extraction_to,
	           v_extraction_from,
	           v_object_name,
	           v_load_method,
	           v_tmp_schema_name,
	           v_delete_physical
	      from fw.load_info li
	      join fw.objects ob on li.object_id = ob.object_id
	      left join fw.objects_archivation oa on li.object_id = oa.object_id
	     where li.load_id = p_load_id;

	    if v_object_name is null then
	      v_error := 'Unable to find load_info/objects for load_id = '||p_load_id;
	      perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                            p_location := v_location, p_load_id := p_load_id);
	      perform fw.f_set_load_id_error(p_load_id := p_load_id);
	      return false;
	    end if;

	    v_full_table_name = fw.f_unify_name(p_name := v_object_name);
	    v_schema_name_origin = fw.f_get_table_schema(v_full_table_name);
	    v_table_name = right(v_full_table_name, length(v_full_table_name) - POSITION('.' in v_full_table_name));
	    v_load_method = fw.f_unify_name(p_name := coalesce(v_load_method, ''));

	    IF v_load_method <> 's3_rearchive' then
	      v_error := 'Unable to restore: load_method must be s3_rearchive, got '||coalesce(v_load_method,'empty');
	      perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                            p_location := v_location, p_load_id := p_load_id);
	      perform fw.f_set_load_id_error(p_load_id := p_load_id);
	      return false;
	    END IF;

	    IF v_tmp_schema_name is null or v_tmp_schema_name = '' then
	      v_error := 'Unable to restore: objects_archivation.tmp_schema is empty for '||v_full_table_name;
	      perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                            p_location := v_location, p_load_id := p_load_id);
	      perform fw.f_set_load_id_error(p_load_id := p_load_id);
	      return false;
	    END IF;

	    IF v_extraction_from > v_extraction_to then
	      v_error := 'Unable to restore from ('||v_extraction_from||') to ('||v_extraction_to||') dates';
	      perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                            p_location := v_location, p_load_id := p_load_id);
	      perform fw.f_set_load_id_error(p_load_id := p_load_id);
	      return false;
	    END IF;

	    -- Список колонок родителя для INSERT и отпечаток схемы (имя+тип, по порядку)
	    select string_agg('"'||c.column_name||'"', ', ' order by c.ordinal_position)
	      into v_columns
	      from information_schema.columns c
	     where c.table_schema||'.'||c.table_name = lower(v_full_table_name);

	    select string_agg(
	             '"'||c.column_name||'" '||
	             CASE
	               WHEN c.data_type = 'numeric' AND c.numeric_precision IS NOT NULL
	                 THEN c.data_type||'('||c.numeric_precision||','||coalesce(c.numeric_scale,0)||')'
	               WHEN c.data_type = 'character varying'
	                 THEN c.data_type||'('||c.character_maximum_length||')'
	               ELSE c.data_type
	             END,
	             '| ' order by c.ordinal_position)
	      into v_parent_schema_fp
	      from information_schema.columns c
	     where c.table_schema||'.'||c.table_name = lower(v_full_table_name);

	    SELECT quote_ident(pg_get_userbyid(c.relowner))
	      INTO v_table_owner
	      FROM pg_class c
	      JOIN pg_namespace n ON n.oid = c.relnamespace
	     WHERE n.nspname = lower(v_schema_name_origin)
	       AND c.relname = lower(v_table_name);

	    v_distribution_key = fw.f_get_distribution_key(v_full_table_name);

	    v_cnt_sum := 0;
	    v_res = true;

	    IF v_extraction_type in (select distinct extraction_type from fw.d_extraction_type) then
	      FOR rec IN
	        select partname, partrangestart, partrangeend
	          from fw.f_partition_name_list_by_date(v_object_name, v_extraction_from, v_extraction_to)
	      LOOP
	        v_part_full_name := v_schema_name_origin||'.'||rec.partname;

	        -- Идемпотентность: уже физическая партиция → пропуск
	        if fw.f_check_tab_part_is_insertable_into(v_object_name, rec.partname) is true then
	          perform fw.f_write_log(
	            p_log_type := 'SERVICE',
	            p_log_message := 'INFO partition '||rec.partname||' is already physical (insertable). Skip restore.',
	            p_location := v_location,
	            p_load_id := p_load_id);
	          continue;
	        end if;

	        -- Схема партиции должна совпадать с родителем (DDL на частично архивной таблице запрещён)
	        select string_agg(
	                 '"'||c.column_name||'" '||
	                 CASE
	                   WHEN c.data_type = 'numeric' AND c.numeric_precision IS NOT NULL
	                     THEN c.data_type||'('||c.numeric_precision||','||coalesce(c.numeric_scale,0)||')'
	                   WHEN c.data_type = 'character varying'
	                     THEN c.data_type||'('||c.character_maximum_length||')'
	                   ELSE c.data_type
	                 END,
	                 '| ' order by c.ordinal_position)
	          into v_part_columns
	          from information_schema.columns c
	         where c.table_schema = lower(v_schema_name_origin)
	           and c.table_name = lower(rec.partname);

	        if v_part_columns is distinct from v_parent_schema_fp then
	          v_error := 'Schema mismatch between parent '||v_full_table_name||
	                     ' and archived partition '||v_part_full_name||
	                     '. Restore aborted (DDL on partially archived tables is not allowed).';
	          perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                                p_location := v_location, p_load_id := p_load_id);
	          RAISE NOTICE '%', v_error;
	          perform fw.f_set_load_id_error(p_load_id := p_load_id);
	          return false;
	        end if;

	        v_buffer_table := v_tmp_schema_name||'.'||rec.partname||'_restore';
	        execute 'DROP TABLE IF EXISTS '||v_buffer_table;

	        v_sql_check := 'create table '||v_buffer_table||
	          ' WITH (appendonly=''true'', orientation=''column'', compresstype=zstd, compresslevel=''1'')'||
	          ' as select * from '||v_full_table_name||' where 1=0 '||v_distribution_key;
	        RAISE NOTICE 'Create restore buffer: %', v_sql_check;
	        execute v_sql_check;
	        execute 'ALTER TABLE '||v_buffer_table||' OWNER TO '||v_table_owner;
	        execute 'GRANT ALL ON TABLE '||v_buffer_table||' TO role_fw_owner';

	        -- Загрузка из external-партиции (PXF → S3)
	        v_sql := 'select '||v_columns||' from '||v_part_full_name;
	        RAISE NOTICE 'Restore into % from SQL: %', v_buffer_table, v_sql;
	        v_cnt = null;
	        v_cnt = fw.f_insert_table_sql(p_table_to := v_buffer_table, p_sql := v_sql);

	        if v_cnt is null then
	          v_error := 'Insert from archived partition failed for '||v_part_full_name;
	          perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                                p_location := v_location, p_load_id := p_load_id);
	          execute 'DROP TABLE IF EXISTS '||v_buffer_table;
	          perform fw.f_set_load_id_error(p_load_id := p_load_id);
	          return false;
	        end if;

	        execute 'select count(1) from '||v_part_full_name into v_cnt_compare_origin;
	        execute 'select count(1) from '||v_buffer_table into v_cnt_compare_buffer;

	        if v_cnt_compare_origin is distinct from v_cnt_compare_buffer then
	          v_error := 'Rows in archived partition and restore buffer not equal. Archive '||
	                     v_cnt_compare_origin||' buffer '||v_cnt_compare_buffer||' for '||rec.partname;
	          perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                                p_location := v_location, p_load_id := p_load_id);
	          execute 'DROP TABLE IF EXISTS '||v_buffer_table;
	          perform fw.f_set_load_id_error(p_load_id := p_load_id);
	          return false;
	        end if;

	        perform fw.f_write_log(
	          p_log_type := 'SERVICE',
	          p_log_message := 'INFO switch partition '||rec.partname||' with physical buffer '||v_buffer_table||
	                           ' ('||v_cnt_compare_buffer||' rows)',
	          p_location := v_location,
	          p_load_id := p_load_id);

	        -- Буфер физический → EXCHANGE WITH VALIDATION (p_external := false)
	        PERFORM fw.f_switch_partition(
	             p_table_name       := v_object_name,
	             p_partition_name   := rec.partname,
	             p_switch_table_name := v_buffer_table,
	             p_external         := false);

	        -- После EXCHANGE в v_buffer_table оказывается бывший external-объект партиции
	        if v_delete_physical then
	          RAISE NOTICE 'Drop swapped-out archive object % (delete_physical=true)', v_buffer_table;
	          begin
	            execute 'DROP EXTERNAL TABLE IF EXISTS '||v_buffer_table;
	          exception when others then
	            execute 'DROP TABLE IF EXISTS '||v_buffer_table;
	          end;
	          perform fw.f_write_log(
	            p_log_type := 'SERVICE',
	            p_log_message := 'INFO dropped swapped-out external after restore of '||rec.partname||
	                             '. S3 parquet objects are not deleted by DROP EXTERNAL.',
	            p_location := v_location,
	            p_load_id := p_load_id);
	        else
	          RAISE NOTICE 'Keep swapped-out archive object % (delete_physical=false)', v_buffer_table;
	          perform fw.f_write_log(
	            p_log_type := 'SERVICE',
	            p_log_message := 'INFO keep swapped-out external table '||v_buffer_table||
	                             ' after restore of '||rec.partname,
	            p_location := v_location,
	            p_load_id := p_load_id);
	        end if;

	        v_cnt_sum := v_cnt_sum + coalesce(v_cnt, 0);
	      END LOOP;

	      perform fw.f_update_load_info(
	        p_load_id    := p_load_id,
	        p_field_name := 'row_cnt',
	        p_value      := v_cnt_sum::text);
	    ELSE
	      v_error := 'Unable to process extraction type '||coalesce(v_extraction_type,'empty');
	      perform fw.f_write_log(p_log_type := 'ERROR', p_log_message := v_error,
	                            p_location := v_location, p_load_id := p_load_id);
	      perform fw.f_set_load_id_error(p_load_id := p_load_id);
	      return false;
	    END IF;

	    if v_res is true then
	      perform fw.f_write_log(
	         p_log_type := 'SERVICE',
	         p_log_message := 'END restore archive data for load_id = '||p_load_id||', '||v_cnt_sum||' rows restored',
	         p_location    := v_location,
	         p_load_id     := p_load_id);
	      PERFORM fw.f_set_load_id_success(p_load_id := p_load_id);
	      return true;
	    else
	      PERFORM fw.f_set_load_id_error(p_load_id := p_load_id);
	      return false;
	    end if;

	exception when others then
	     raise notice 'ERROR while restore archive data for load_id = %: %', p_load_id, SQLERRM;
	     PERFORM fw.f_write_log(
	        p_log_type    := 'ERROR',
	        p_log_message := 'Restore archive data for load_id '||p_load_id||' finished with error: '||SQLERRM,
	        p_location    := v_location,
	        p_load_id     := p_load_id);
	     begin
	       if v_buffer_table is not null then
	         execute 'DROP TABLE IF EXISTS '||v_buffer_table;
	       end if;
	     exception when others then
	       null;
	     end;
	     perform fw.f_set_load_id_error(p_load_id := p_load_id);
	     return false;
	END;



$$
EXECUTE ON ANY;
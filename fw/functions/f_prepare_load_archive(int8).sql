-- DROP FUNCTION fw.f_prepare_load_archive(int8);

CREATE OR REPLACE FUNCTION fw.f_prepare_load_archive(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	


    /*Ismailov Dmitry + Solovev D (oct 2024)
    * Sapiens Solutions
    * 2023*/
	/*preparing operations before loading*/
	DECLARE
	  v_location        text := 'fw.f_prepare_load_archive';
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
      rec               record;
 	BEGIN
 	/*created from hdset 2025-03-13*/
    --Log
    perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'START Before load processing for table '||v_table_name,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call

	--set load_id for session
    --perform set_config('fw.load_id', p_load_id::text, false);
    perform fw.f_set_session_param(
       p_param_name  := 'fw.load_id',
       p_param_value := p_load_id::text);
    perform fw.f_set_load_id_in_process(p_load_id := p_load_id);
    -- Get table load type
    select ob.object_id, ob.object_name,
    coalesce(li.extraction_type, ob.extraction_type),
    coalesce(li.load_type, ob.load_type),
    coalesce(li.load_method, ob.load_method),
    ob.load_interval, li.extraction_from::date,
    least(date_trunc('month', (li.extraction_to + interval '1' day)::date) - interval '1' day,
    	  date_trunc('month', (current_date - period_of_physical_live_in_month * INTERVAL '1 month')::date) - interval '1' day	
    		)::date,
    oa.tmp_schema
    into   v_object_id, v_full_table_name, v_extraction_type ,v_load_type, v_load_method, v_load_interval, v_extraction_from, v_extraction_to, v_tmp_schema_name
    from   fw.load_info li
    join   fw.objects ob on(ob.object_Id = li.object_Id)
    left join fw.objects_archivation oa on li.object_id = oa.object_id
    where  li.load_id = p_load_id;

    perform fw.f_update_load_info(
             p_load_id    := p_load_id,
             p_field_name := 'extraction_to',
             p_value      := v_extraction_to::timestamp::text);

    v_full_table_name  = fw.f_unify_name(p_name := v_full_table_name); -- full table name
    v_schema_name = fw.f_get_table_schema(v_full_table_name);--left(v_full_table_name,position('.' in v_full_table_name)-1); -- target table schema name
    v_schema_name_origin = fw.f_get_table_schema(v_full_table_name);
    v_table_name =  right(v_full_table_name,length(v_full_table_name) - POSITION('.' in v_full_table_name));-- table name wo schema
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

    IF v_load_type in (select distinct load_type from fw.d_load_type) and v_extraction_type in (select distinct extraction_type from fw.d_extraction_type) THEN
        --Creates work tables
    raise notice '1. Start creating external table with params: p_table_name: {%}, p_load_method: {%}, p_schema_name: {%}',
                  coalesce(v_full_table_name,'{empty}'),coalesce(v_load_method,'{empty}'),coalesce(v_tmp_schema_name,'{empty}');
       /* PERFORM fw.f_create_ext_table(
           p_table_name  := v_full_table_name,
           p_load_method := v_load_method,
           p_schema_name := v_tmp_schema_name,
           p_prefix := v_ext_prefix,
           p_suffix := v_ext_suffix,
           p_load_id := p_load_id);*/
      FOR rec IN
        select * from fw.f_partition_name_list_by_date(v_full_table_name, v_extraction_from, v_extraction_to)
     /* select * from fw.f_partition_name_list_by_date('src_hybris.customertracking$2',
      '2024-07-07', '2024-07-07')*/

      LOOP
        if fw.f_check_tab_part_is_insertable_into(v_full_table_name, rec.partname) is true then --Проверим, что рассматриваемая партиция все еще физическая = в нее можно записывать данные. Если в нее нельзя записывать данные, то она уже EXTERNAL и создавать ее повторно не нужно
          RAISE NOTICE 'Creating external writable table %', rec.partname;
          perform fw.f_write_log(
                         p_log_type    := 'SERVICE',
                         p_log_message := 'INFO Creating external writable table '||rec.partname,
						 p_location    := v_location,
                         p_load_id     := p_load_id);
          PERFORM fw.f_create_ext_table(p_table_name := v_schema_name_origin||'.'||rec.partname/*'src_hybris.customertracking$2'*/,
                                        p_load_id := p_load_id,
                                        p_load_method := v_load_method,
                                        p_schema_name := v_tmp_schema_name,
                                        p_is_writeble_type := true);
        else perform fw.f_write_log(
                         p_log_type    := 'SERVICE',
                         p_log_message := 'INFO partition '||rec.partname||' is already external. Please check it.',
                         p_location    := v_location,
                         p_load_id     := p_load_id);
        end if;
      END LOOP;


   /* raise notice '2. Start creating delta table with params: p_table_name: {%}, p_schema_name: {%}, p_prefix_name: {%}',
                  coalesce(v_full_table_name,'{empty}'),coalesce(v_tmp_schema_name,'{empty}'),coalesce(v_tmp_prefix,'{empty}');
		PERFORM fw.f_create_tmp_table(
	       p_table_name  := v_full_table_name,
	       p_schema_name := v_tmp_schema_name,
	       p_prefix_name := v_tmp_prefix,
	       p_suffix_name := v_tmp_suffix,
	       p_drop_table  := true);*/
	    v_res = true;
    ELSE
        v_error := 'Unable to process extraction ('||coalesce(v_extraction_type,'empty')||') or load ('||coalesce(v_load_type,'empty')||') types ';
        perform fw.f_write_log(
           p_log_type    := 'ERROR',
           p_log_message := 'Error while processing before job tasks: '||v_error,
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        return false;
    END IF;

  -- Log
  perform fw.f_write_log(
     p_log_type    := 'SERVICE',
     p_log_message := 'END Before job tasks processing for table '||v_full_table_name,
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
  return v_res;
 exception when others then
     raise notice 'ERROR while prepare loading table %: %',v_table_name,SQLERRM;
     PERFORM fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := 'Prepare loading into table '||v_full_table_name||' finished with error: '||SQLERRM,
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);
     return false;
END;



$$
EXECUTE ON ANY;
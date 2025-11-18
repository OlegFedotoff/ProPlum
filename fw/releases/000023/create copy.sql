    DROP FUNCTION IF EXISTS fw.f_get_table_count(text);
    DROP FUNCTION IF EXISTS fw.f_process_date_string(text);
    DROP FUNCTION IF EXISTS fw.f_process_extract_odata(int8);
    DROP FUNCTION IF EXISTS fw.f_post_extract_odata(int8);
    DROP FUNCTION IF EXISTS fw.f_prepare_extract_odata(int8);

CREATE TABLE fw.odata_query_helper (
	load_id int8 NOT NULL,
	sql_query text NOT NULL,
	table_to text NOT NULL,
	rows_count int8 NOT NULL,
	delta_field text NULL,
	extraction_to timestamp NULL
)
DISTRIBUTED BY (load_id);

INSERT INTO fw.d_load_method (load_method, desc_short, desc_middle, desc_long) 
VALUES('zservice_async', 'Загрузка через zservice в асинхронном режиме', 'Load from zservice async', NULL);

CREATE OR REPLACE FUNCTION fw.f_get_table_count(table_name text)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    cnt bigint;
BEGIN
    EXECUTE 'SELECT COALESCE(sum(reltuples), 0)::bigint as total_rows
             FROM pg_class 
             JOIN pg_namespace ON relnamespace = pg_namespace.oid
             WHERE relkind = ''r''
             AND nspname||''.''||relname LIKE ' || quote_literal(table_name || '%') INTO cnt;
    RETURN cnt;
END;

$$
EXECUTE ON ANY;

CREATE OR REPLACE FUNCTION fw.f_process_date_string(input_string text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    result_string TEXT := input_string;
    matches TEXT[];
    days_back INTEGER;
    replacement_date TEXT;
BEGIN
    -- Обрабатываем все вхождения current_date - N
    LOOP
        -- Ищем паттерн current_date - число
        SELECT array_agg(m[1]) INTO matches 
        FROM regexp_matches(result_string, 'current_date\s*-\s*(\d+)', 'g') AS m;
        
        EXIT WHEN matches IS NULL OR array_length(matches, 1) = 0;
        
        -- Обрабатываем первое найденное вхождение
        days_back := CAST(matches[1] AS INTEGER);
        replacement_date := '''' || TO_CHAR(CURRENT_DATE - days_back, 'YYYYMMDD') || '''';
        
        -- Заменяем первое вхождение
        result_string := REGEXP_REPLACE(
            result_string,
            'current_date\s*-\s*' || days_back::TEXT,
            replacement_date,
            ''  -- заменяем только первое вхождение
        );
    END LOOP;
    
    -- Заменяем оставшиеся current_date
    result_string := REGEXP_REPLACE(
        result_string,
        'current_date',
        '''' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '''',
        'g'
    );
    
    RETURN result_string;
END;

$$
EXECUTE ON ANY;

-- DROP FUNCTION fw.f_process_extract_odata(int8);

CREATE OR REPLACE FUNCTION fw.f_process_extract_odata(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
    /*New function*/
DECLARE
    v_location            text := 'fw.f_process_extract_odata';
    v_extraction_type     text;
    v_extraction_to       timestamp;
    v_extraction_from     timestamp;
    v_tmp_table_name      text;
    v_ext_table_name      text;
    v_delta_field         text;
    v_error               text;
    v_sql                 text;
    v_where               text;
    v_res                 bool;
    v_cnt                 int8;
    v_cnt_prev			  int8;
    v_server              text;
BEGIN
	perform fw.f_write_log(p_log_type := 'SERVICE', 
       p_log_message := 'START extract data for load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
   	--set load_id for session
       select oqh.sql_query, oqh.table_to, oqh.rows_count from fw.v_odata_query_helper oqh where oqh.load_id = p_load_id
       into v_sql, v_tmp_table_name, v_cnt_prev;
--       for update; --Заблокировать строку 
       raise notice 'v_cnt_prev is %', v_cnt_prev;
       v_server = lower(fw.f_get_constant('c_log_fdw_server'));
       if v_cnt_prev < 0 or v_cnt_prev is null then 
       		raise 'Previous thread was fallen. p_load_id is: %', p_load_id;
       end if;
        v_cnt =  fw.f_insert_table_sql(
           p_table_to := v_tmp_table_name,
           p_sql      := v_sql); --load from ext to stage (delta) table
        if v_cnt is not null then
         v_res = true;
        else 
         v_res = false;
        end if;  
    if v_res is true then
      -- Log Success
      perform fw.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END extract data for load_id = '||p_load_id||', '||v_cnt||' rows extracted',
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      v_sql = 'update fw.odata_query_helper set rows_count = '||(v_cnt + v_cnt_prev)||' where load_id = ' || p_load_id::text;
      perform dblink(v_server,v_sql);
      return v_res;
    else 
      perform fw.f_set_load_id_error(p_load_id := p_load_id);
      -- Log errors
      perform fw.f_write_log(
         p_log_type := 'SERVICE', 
         p_log_message := 'END extract data for load_id = '||p_load_id||' finished with error', 
         p_location    := v_location,
         p_load_id     := p_load_id); --log function call
      v_sql = 'update fw.odata_query_helper set rows_count = -1 where load_id = ' || p_load_id::text;
      perform dblink(v_server,v_sql);
      perform fw.f_set_load_id_error(p_load_id := p_load_id);
      return false;
     end if;
    return false;
   
    exception when others then 
     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
     perform fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Extract data for load_id '||p_load_id||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);  
     raise 'Current thread was fallen. p_load_id is: %', p_load_id;
     return false;
END;



$$
EXECUTE ON ANY;

-- DROP FUNCTION fw.f_post_extract_odata(int8);

CREATE OR REPLACE FUNCTION fw.f_post_extract_odata(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
    /*New function*/
DECLARE
    v_location            text := 'fw.f_post_extract_odata';
    v_extraction_type     text;
    v_extraction_to       timestamp;
    v_extraction_from     timestamp;
    v_tmp_table_name      text;
    v_ext_table_name      text;
    v_delta_field         text;
    v_error               text;
    v_sql                 text;
    v_where               text;
    v_res                 bool;
    v_cnt                 int8;
BEGIN
		select oqh.rows_count, oqh.delta_field, oqh.extraction_to from fw.odata_query_helper oqh where oqh.load_id = p_load_id  
		into v_cnt, v_delta_field, v_extraction_to ;
		if v_cnt is null or v_cnt < 0 
		then
        	perform fw.f_set_load_id_error(p_load_id := p_load_id);
	    	perform fw.f_write_log(
	        	 p_log_type := 'SERVICE', 
	         	p_log_message := 'END extract data for load_id = '||p_load_id||' finished with error', 
	         	p_location    := v_location,
	         	p_load_id     := p_load_id); --log function call
	       return false;
        else 
           perform fw.f_update_load_info(
             p_load_id    := p_load_id, 
             p_field_name := 'extraction_to', 
             p_value      := coalesce(fw.f_get_max_value(v_tmp_table_name,v_delta_field),v_extraction_to::text));
	 	   perform fw.f_write_log(
	         p_log_type := 'SERVICE', 
	         p_log_message := 'END extract data for load_id = '||p_load_id||', '||v_cnt||' rows extracted',
	         p_location    := v_location,
	         p_load_id     := p_load_id); --log function call
	       return true;
      -- Log errors
     	end if;
    exception when others then 
     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
     perform fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Extract data for load_id '||p_load_id||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);  
     return false;
END;

$$
EXECUTE ON ANY;

-- DROP FUNCTION fw.f_prepare_extract_odata(int8);

CREATE OR REPLACE FUNCTION fw.f_prepare_extract_odata(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
    /*New function*/
DECLARE
    v_location            text := 'fw.f_prepare_extract_odata';
    v_extraction_type     text;
    v_extraction_to       timestamp;
    v_extraction_from     timestamp;
    v_tmp_table_name      text;
    v_ext_table_name      text;
    v_delta_field         text;
    v_error               text;
    v_sql                 text;
    v_where               text;
    v_res                 text;
    v_cnt                 int8;
    v_server              text;
BEGIN
	perform fw.f_write_log(p_log_type := 'SERVICE', 
       p_log_message := 'START prepare extract odata data for load_id = '||p_load_id, 
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
   	--set load_id for session
    perform fw.f_set_session_param(
       p_param_name  := 'fw.load_id', 
       p_param_value := p_load_id::text);
    perform fw.f_set_load_id_in_process(
       p_load_id := p_load_id);
    -- Get table load type
    v_sql := 'select coalesce(li.extraction_type, ob.extraction_type), 
              case coalesce(li.extraction_type, ob.extraction_type) 
                when ''DELTA'' then ob.delta_field
                when ''PARTITION'' then ob.bdate_field
                else coalesce(ob.delta_field,ob.bdate_field,null)::text
              end,
              li.extraction_to,
              li.extraction_from
              from fw.load_info li, fw.objects ob where li.object_id = ob.object_id and li.load_id = ' ||
             p_load_id::text;
    execute v_sql into v_extraction_type, v_delta_field, v_extraction_to, v_extraction_from;
    v_server = lower(fw.f_get_constant('c_log_fdw_server'));
    v_tmp_table_name = fw.f_get_delta_table_name(p_load_id := p_load_id);
    v_ext_table_name = fw.f_get_ext_table_name(p_load_id := p_load_id);
    v_where = fw.f_get_extract_where_cond(p_load_id := p_load_id);
    -- process where clause
    IF v_extraction_type in (select distinct extraction_type from fw.d_extraction_type) 
     then
        perform  fw.f_truncate_table(p_table_name := v_tmp_table_name);
        v_sql := replace(fw.f_get_load_expression(p_load_id := p_load_id)||' where '||v_where,'''','''''');
        v_sql := fw.f_replace_variables(p_load_id,v_sql);
        v_sql := 'INSERT INTO fw.odata_query_helper 
                   VALUES ( '||p_load_id||' , '''||v_sql||''', '''||v_tmp_table_name||''', '||0||', '||coalesce(''''||v_delta_field||'''','NULL')||', '''||v_extraction_to||''');';
        --dblink for fdw server
        raise notice 'insert sql into query helper: = %',v_sql;
        v_res := dblink(v_server,v_sql);
        --insert into fw.odata_query_helper values (p_load_id, v_sql, v_tmp_table_name, 0, v_delta_field, v_extraction_to);
     return true;
     ELSE
        v_error := 'Unable to process extraction type '||v_extraction_type;
        perform fw.f_write_log(
           p_log_type := 'ERROR', 
           p_log_message := 'Error while extraction: ' || v_error, 
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '%',v_error;
        perform fw.f_set_load_id_error(p_load_id := p_load_id);
        return false;
    END IF;  

    exception when others then 
     raise notice 'ERROR while extract data for load_id = %: %',p_load_id,SQLERRM;
     perform fw.f_write_log(
        p_log_type    := 'ERROR', 
        p_log_message := 'Extract data for load_id '||p_load_id||' finished with error: '||SQLERRM, 
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);  
     return false;
END;

$$
EXECUTE ON ANY;




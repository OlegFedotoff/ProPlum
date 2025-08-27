-- DROP FUNCTION fw.f_get_connection_string(int8, bool, text);

CREATE OR REPLACE FUNCTION fw.f_get_connection_string(p_load_id int8, p_is_writeble_type bool DEFAULT false, p_part_name text DEFAULT ''::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	


    /*Ismailov Dmitry + Solovev D. (march of 2025)
    * Sapiens Solutions
    * 2023*/
	/*get connection string for external table*/
declare
    v_location text := 'fw.f_get_connection_string';
    v_extraction_type text;
    v_load_method text;
    v_conn_str text;
    v_sql_conn text;
    v_part_string text;
    v_start_date timestamp;
    v_end_date timestamp;
    v_delta_fld text;
    v_bdate_fld text;
    v_bdate_safety_period text;
    v_error text;
    v_table_name text;
	--v_date_txt text;
    v_part_name text;
    v_archive_write_conn text;
    v_archive_read_conn text;
begin
    /*created  from hdset 2025-03-13*/
	--Function returns connection string for external table, also checks settings for external tables in table fw.ext_tables_params
    perform fw.f_write_log(
       p_log_type    := 'SERVICE',
       p_log_message := 'START Get connection string for load_id = ' || p_load_id,
       p_location    := v_location,
       p_load_id     := p_load_id);
    select li.extraction_type, coalesce(etp.load_method,ob.load_method), coalesce(etp.connection_string, ob.connect_string),
           li.extraction_from, li.extraction_to, ob.delta_field, ob.bdate_field, ob.bdate_safety_period
    , replace(substring(ob.object_name, position('.' in ob.object_name) + 1, length(ob.object_name)), '$', ''),
     oa.write_connect_string as write_conn, oa.read_connect_string as read_conn
     from  fw.load_info li
      join fw.objects ob on ob.object_id = li.object_id
      left join fw.ext_tables_params etp on ob.object_id = etp.object_id and etp."active" is true
      left join fw.objects_archivation oa on li.object_id = oa.object_id
     where li.load_id =  p_load_id
    into v_extraction_type, v_load_method, v_conn_str,
		 v_start_date, v_end_date, v_delta_fld,
		 v_bdate_fld, v_bdate_safety_period, v_table_name,
         v_archive_write_conn, v_archive_read_conn;

    v_part_name = coalesce(p_part_name, '');

    if coalesce(v_conn_str,'') = '' and v_load_method <> 's3_archive' then
     perform fw.f_write_log(
       p_log_type    := 'SERVICE',
       p_log_message := 'Connection string for load_id = ' || p_load_id||' is empty',
       p_location    := v_location,
       p_load_id     := p_load_id);
     RAISE notice 'Connection string for load_id = % is empty',p_load_id;
     return null::text;
    end if;

    --Если выполняется архивация таблицы, то у нее должны быть установлены коннекты для создания таблицы на запись в S3 (указывается в objects.connect_string) и на чтение (указывается в ext_table_params.connection_string)
    if v_load_method = 's3_archive' and (v_archive_write_conn is null or v_archive_read_conn is null) then
      perform fw.f_write_log(
        p_log_type    := 'SERVICE',
        p_log_message := 'Some connection strings for s3_archibe type of load_id = ' || p_load_id||' is empty',
        p_location    := v_location,
        p_load_id     := p_load_id);
      RAISE notice 'Connection string for load_id = % is empty',p_load_id;
      v_error := 'Unknown archive connection v_archive_write_conn='||v_archive_write_conn||', v_archive_read_conn='||v_archive_read_conn;
      perform fw.f_write_log(
         p_log_type    := 'ERROR',
         p_log_message := v_error,
         p_location    := v_location); --log function call
      RAISE EXCEPTION '%',v_error;

      --return null::text;
    end if;

    --Если выполняется архивация таблицы, то должна быть указана партиция, которая архивируется
    if v_load_method = 's3_archive' and v_part_name = '' then
      perform fw.f_write_log(
        p_log_type    := 'SERVICE',
        p_log_message := 'Partition name for s3_archibe type of load_id = ' || p_load_id||' is empty',
        p_location    := v_location,
        p_load_id     := p_load_id);
      RAISE notice 'Partition name for load_id = % is empty',p_load_id;
      v_error := 'Unknown partition name for archive load.';
      perform fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := v_error,
        p_location    := v_location); --log function call
      RAISE EXCEPTION '%',v_error;
     --return null::text;
    end if;

    v_bdate_safety_period = replace(replace(replace(v_bdate_safety_period,' day',':day'),' mon',':month'),' year',':year');
    v_conn_str =  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(v_conn_str,
                  '$load_id',coalesce(p_load_id::text,''::text)),
                  '$current_date',to_char(current_date,'YYYY-MM-DD')),
                  '$current_date_YYYYMMDD',to_char(current_date,'YYYYMMDD')),
                  '$now',to_char(now(),'YYYY-MM-DD')),
                  '$extraction_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$extraction_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$load_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$load_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$delta_field',coalesce(v_delta_fld,'')::text),
                  '$bdate_safety_period',coalesce(v_bdate_safety_period,'')::text),
                  '$bdate_field',coalesce(v_bdate_fld,'')::text);
    RAISE notice 'v_conn_str: %',coalesce(v_conn_str,'empty');
    v_archive_write_conn =  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(v_archive_write_conn,
                  '$load_id',coalesce(p_load_id::text,''::text)),
                  '$current_date',to_char(current_date,'YYYY-MM-DD')),
                  '$now',to_char(now(),'YYYY-MM-DD')),
                  '$extraction_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$extraction_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$load_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$load_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$delta_field',coalesce(v_delta_fld,'')::text),
                  '$bdate_safety_period',coalesce(v_bdate_safety_period,'')::text),
                  '$bdate_field',coalesce(v_bdate_fld,'')::text),
                  '$partition_name',coalesce(v_part_name,'')::text);
    RAISE notice 'v_archive_write_conn: %',coalesce(v_archive_write_conn,'empty');
    v_archive_read_conn =  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(v_archive_read_conn,
                  '$load_id',coalesce(p_load_id::text,''::text)),
                  '$current_date',to_char(current_date,'YYYY-MM-DD')),
                  '$now',to_char(now(),'YYYY-MM-DD')),
                  '$extraction_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$extraction_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$load_from',to_char(v_start_date,'YYYY-MM-DD')::text),
                  '$load_to',to_char(v_end_date,'YYYY-MM-DD')::text),
                  '$delta_field',coalesce(v_delta_fld,'')::text),
                  '$bdate_safety_period',coalesce(v_bdate_safety_period,'')::text),
                  '$bdate_field',coalesce(v_bdate_fld,'')::text),
                  '$partition_name',coalesce(v_part_name,'')::text);
    RAISE notice 'v_archive_read_conn: %',coalesce(v_archive_read_conn,'empty');

	case v_load_method
     when 'gpfdist' then
      v_sql_conn := v_conn_str;
     when 'dblink' then
      v_sql_conn := v_conn_str;
     when 'pxf' then
      v_part_string = coalesce(fw.f_get_pxf_partition(p_load_id := p_load_id),'');
      v_sql_conn :=
      'LOCATION (''pxf://'||v_conn_str||v_part_string||''') ON ALL FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'' )
       ENCODING ''UTF8''';
     when 'python' then
    -- no need to create external table for python load method
      perform fw.f_write_log(
         p_log_type    := 'SERVICE',
         p_log_message := 'END Creating external table for table '||v_full_table_name|| '. No need to create external table for python load method',
         p_location    := v_location); --log function call
      return ''::text;
     when 's3_archive' then
	  case when p_is_writeble_type
		then v_sql_conn := v_archive_write_conn;
		else v_sql_conn := v_archive_read_conn;
      END CASE;
	 else
      v_error := 'Unknown load method '|| v_load_method;
      perform fw.f_write_log(
         p_log_type    := 'ERROR',
         p_log_message := v_error,
         p_location    := v_location); --log function call
      RAISE EXCEPTION '%',v_error;
     end case;
    perform fw.f_write_log(
        p_log_type    := 'SERVICE',
        p_log_message := 'END Get connection string for load_id = ' || p_load_id||', connect string: '||v_sql_conn,
        p_location    := v_location,
        p_load_id     := p_load_id);
    return v_sql_conn;
END;



$$
EXECUTE ON ANY;
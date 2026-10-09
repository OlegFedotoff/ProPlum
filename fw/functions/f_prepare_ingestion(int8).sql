-- DROP FUNCTION fw.f_prepare_ingestion(int8);

CREATE OR REPLACE FUNCTION fw.f_prepare_ingestion(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

	/*Функция подготовки ingestion-загрузки объекта с load_method = zservice_raw
	(см. dags/dags_it/dags_load_crm/docs/TARGET_ARCHITECTURE.md, раздел 15.2).
	Создана на основе fw.f_prepare_extract_odata.
	Работает с заранее созданной постоянной RAW-таблицей из fw.objects.object_name:
	очищает её и идемпотентно заменяет строку fw.odata_query_helper.
	Не получает имена временной delta/external-таблиц, не формирует SQL
	переноса данных и не создаёт таблицы.*/
DECLARE
    v_location        text := 'fw.f_prepare_ingestion';
    v_extraction_type text;
    v_delta_field     text;
    v_extraction_to   timestamp;
    v_object_name     text;
    v_error           text;
    v_server          text;
    v_sql             text;
    v_res             text;
BEGIN
	perform fw.f_write_log(p_log_type := 'SERVICE',
       p_log_message := 'START prepare ingestion for load_id = '||p_load_id,
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
   	--set load_id for session
    perform fw.f_set_session_param(
       p_param_name  := 'fw.load_id',
       p_param_value := p_load_id::text);
    -- Get load_info and objects attributes
    select coalesce(li.extraction_type, ob.extraction_type),
           case coalesce(li.extraction_type, ob.extraction_type)
                when 'DELTA' then ob.delta_field
                when 'PARTITION' then ob.bdate_field
                else coalesce(ob.delta_field, ob.bdate_field, null)::text
           end,
           li.extraction_to,
           ob.object_name
      into v_extraction_type, v_delta_field, v_extraction_to, v_object_name
      from fw.load_info li, fw.objects ob
     where li.object_id = ob.object_id
       and li.load_id = p_load_id;
    -- Check qualified name of RAW table
    if v_object_name is null or position('.' in v_object_name) = 0 then
        v_error := 'Unable to prepare ingestion: RAW table name '||coalesce(v_object_name, '{empty}')||' is not a qualified name (schema.table) for load_id = '||p_load_id;
        perform fw.f_write_log(
           p_log_type := 'ERROR',
           p_log_message := 'Error while prepare ingestion: ' || v_error,
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '%', v_error;
        perform fw.f_set_load_id_error(p_load_id := p_load_id);
        return false;
    end if;
    -- Check RAW table existence
    if not fw.f_table_exists(p_table_name := v_object_name) then
        v_error := 'Unable to prepare ingestion: RAW table '||v_object_name||' does not exist for load_id = '||p_load_id;
        perform fw.f_write_log(
           p_log_type := 'ERROR',
           p_log_message := 'Error while prepare ingestion: ' || v_error,
           p_location    := v_location,
           p_load_id     := p_load_id); --log function call
        RAISE NOTICE '%', v_error;
        perform fw.f_set_load_id_error(p_load_id := p_load_id);
        return false;
    end if;
    perform fw.f_set_load_id_in_process(
       p_load_id := p_load_id);
    -- Truncate RAW table
    perform fw.f_truncate_table(p_table_name := v_object_name);
    -- Idempotent replace of fw.odata_query_helper row (same way as in f_prepare_extract_odata)
    v_server = fw.f_get_constant('c_log_fdw_server');
    v_sql := 'DELETE FROM fw.odata_query_helper WHERE load_id = '||p_load_id::text;
    v_res := dblink(v_server, v_sql);
    v_sql := 'INSERT INTO fw.odata_query_helper
              VALUES ( '||p_load_id::text||', '''', '''||v_object_name||''', '||0||', '||
              coalesce(''''||v_delta_field||'''', 'NULL')||', '||
              coalesce(''''||v_extraction_to::text||'''', 'NULL')||');';
    --dblink for fdw server
    raise notice 'insert sql into query helper: = %', v_sql;
    v_res := dblink(v_server, v_sql);
    perform fw.f_write_log(p_log_type := 'SERVICE',
       p_log_message := 'END prepare ingestion for load_id = '||p_load_id||', RAW table '||v_object_name||' is ready for load',
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
    return true;

    exception when others then
     raise notice 'ERROR while prepare ingestion for load_id = %: %', p_load_id, SQLERRM;
     perform fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := 'Prepare ingestion for load_id '||p_load_id||' finished with error: '||SQLERRM,
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);
     return false;
END;

$$
EXECUTE ON ANY;
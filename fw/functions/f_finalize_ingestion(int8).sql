-- DROP FUNCTION fw.f_finalize_ingestion(int8);

CREATE OR REPLACE FUNCTION fw.f_finalize_ingestion(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

	/*Функция завершения ingestion-загрузки объекта с load_method = zservice_raw
	(см. dags/dags_it/dags_load_crm/docs/TARGET_ARCHITECTURE.md, раздел 15.3).
	Создана на основе fw.f_post_extract_odata.
	Данные уже записаны loader-ом в постоянную RAW-таблицу, поэтому перенос
	между таблицами и последующий вызов fw.f_load_data не выполняются:
	функция сразу завершает первичную загрузку.
	Не переносит данные между таблицами, не изменяет extraction range,
	не управляет target locks и не выполняет load_after_function.*/
DECLARE
    v_location text := 'fw.f_finalize_ingestion';
    v_cnt      int8;
BEGIN
	perform fw.f_write_log(p_log_type := 'SERVICE',
       p_log_message := 'START finalize ingestion for load_id = '||p_load_id,
       p_location    := v_location,
       p_load_id     := p_load_id); --log function call
	-- Read load result from fw.odata_query_helper
	select oqh.rows_count
	  into v_cnt
      from fw.odata_query_helper oqh
     where oqh.load_id = p_load_id;
	if v_cnt is null or v_cnt < 0
	then
		perform fw.f_write_log(
        	 p_log_type := 'ERROR',
         	p_log_message := 'END finalize ingestion for load_id = '||p_load_id||' finished with error: no rows_count in fw.odata_query_helper or rows_count < 0',
         	p_location    := v_location,
         	p_load_id     := p_load_id); --log function call
	    perform fw.f_set_load_id_error(p_load_id := p_load_id);
	    return false;
    else
    	-- Write rows count to fw.load_info.row_cnt
	 	perform fw.f_update_load_info(
            p_load_id    := p_load_id,
            p_field_name := 'row_cnt',
            p_value      := v_cnt::text);
	    perform fw.f_set_load_id_success(p_load_id := p_load_id);
	 	perform fw.f_write_log(
	         p_log_type := 'SERVICE',
	         p_log_message := 'END finalize ingestion for load_id = '||p_load_id||', '||v_cnt||' rows loaded',
	         p_location    := v_location,
	         p_load_id     := p_load_id); --log function call
	    return true;
    end if;
    exception when others then
     raise notice 'ERROR while finalize ingestion for load_id = %: %', p_load_id, SQLERRM;
     perform fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := 'Finalize ingestion for load_id '||p_load_id||' finished with error: '||SQLERRM,
        p_location    := v_location,
        p_load_id     := p_load_id);
     perform fw.f_set_load_id_error(p_load_id := p_load_id);
     return false;
END;

$$
EXECUTE ON ANY;
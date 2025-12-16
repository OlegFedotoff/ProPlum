CREATE OR REPLACE FUNCTION fw.f_analyze_table_partitions(
    p_load_id int8,
    p_table_name text,
    p_days int
    
)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_location      text := 'fw.f_analyze_table_partitions';
    v_table_name    text;
    v_schema        text;
    v_start_date    timestamp := now() - (p_days || ' days')::interval;
    v_end_date      timestamp := now();
    rep record;
	v_part_name     text;
BEGIN

    -- Normalize table name (schema.table)
    v_table_name := fw.f_unify_name(p_name := p_table_name);
    v_schema := split_part(v_table_name, '.', 1);

    PERFORM fw.f_write_log(
        p_log_type    := 'SERVICE',
        p_log_message := 'START analyze partitions for table ' || v_table_name ||
                         ', days_back=' || p_days,
        p_location    := v_location,
        p_load_id     := p_load_id
    );

    -- Iterate over partitions
    FOR rep IN (
        SELECT *
        FROM fw.f_partition_name_list_by_date(
                v_table_name,
                v_start_date,
                v_end_date
             ))
     LOOP
        BEGIN  -- обработка ошибок на уровне партиции

            v_part_name := v_schema || '.' || rep.partname;

            PERFORM fw.f_write_log(
                p_log_type    := 'SERVICE',
                p_log_message := 'Analyze partition: ' || v_part_name,
                p_location    := v_location,
                p_load_id     := p_load_id
            );

			
            PERFORM fw.f_analyze_table(
                p_table_name := v_part_name
            );

        EXCEPTION WHEN OTHERS THEN
            PERFORM fw.f_write_log(
                p_log_type    := 'ERROR',
                p_log_message := 'Error analyzing partition ' || v_part_name || ': ' || SQLERRM,
                p_location    := v_location,
                p_load_id     := p_load_id
            );
        END;  -- END блока обработки партиции
    END LOOP;

    PERFORM fw.f_write_log(
        p_log_type    := 'SERVICE',
        p_log_message := 'END analyze partitions for table ' || v_table_name,
        p_location    := v_location,
        p_load_id     := p_load_id
    );

    RETURN true;

EXCEPTION
    WHEN OTHERS THEN
        PERFORM fw.f_write_log(
            p_log_type    := 'ERROR',
            p_log_message := 'Run ' || v_location ||
                             ' finished with error: ' || SQLERRM,
            p_location    := v_location,
            p_load_id     := p_load_id
        );
        RETURN false;

END;
$$
EXECUTE ON ANY;


    CREATE OR REPLACE VIEW fw.v_odata_query_helper
    AS SELECT t1.load_id,
        t1.sql_query,
        t1.table_to,
        t1.rows_count,
        t1.delta_field,
        t1.extraction_to
    FROM dblink('adwh_server'::text, 'SELECT load_id, sql_query, table_to, rows_count, delta_field, extraction_to FROM fw.odata_query_helper'::text) t1(load_id integer, sql_query text, table_to text, rows_count bigint, delta_field text, extraction_to timestamp without time zone) 
    ;


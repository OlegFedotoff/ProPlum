-- DROP FUNCTION fw.f_get_partition_key(text);

CREATE OR REPLACE FUNCTION fw.f_get_partition_key(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function return partition field name*/
DECLARE
    v_location text := 'fw.f_get_partition_key';
    v_table_name text;
    v_part_attr_name text;
BEGIN
    
    v_table_name = fw.f_unify_name(p_table_name);
    -- Get partition key column
    select columnname
    into v_part_attr_name
    from pg_catalog.pg_partition_columns
    where lower(schemaname||'.'||tablename) = lower(v_table_name);
    PERFORM fw.f_write_log('DEBUG', 'v_part_attr_name:{'||coalesce(v_part_attr_name,'empty')||'}', v_location);
    RETURN v_part_attr_name;

END;



$$
EXECUTE ON ANY;
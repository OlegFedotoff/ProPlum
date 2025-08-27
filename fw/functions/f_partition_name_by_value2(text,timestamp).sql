-- DROP FUNCTION fw.f_partition_name_by_value2(text, timestamp);

CREATE OR REPLACE FUNCTION fw.f_partition_name_by_value2(p_table_name text, p_partition_value timestamp)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function returns partition name of the table by it's value*/
DECLARE
  v_location text := 'fw.f_partition_name_by_value';
  v_table_name text;
  v_partition_name text;
BEGIN
  v_table_name = fw.f_unify_name(p_name := p_table_name);
  select max(partname) from fw.f_partition_name_list_by_date(v_table_name,p_partition_value,p_partition_value)
  into v_partition_name;
  PERFORM fw.f_write_log(
    p_log_type    := 'DEBUG', 
    p_log_message := 'v_partition_name:{'||v_partition_name||'}', 
    p_location    :=  v_location);
  RETURN v_partition_name;

END;


$$
EXECUTE ON ANY;
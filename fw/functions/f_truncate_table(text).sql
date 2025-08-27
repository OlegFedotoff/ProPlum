-- DROP FUNCTION fw.f_truncate_table(text);

CREATE OR REPLACE FUNCTION fw.f_truncate_table(p_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	 /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*truncate table*/
DECLARE
  v_location       text := 'fw.f_truncate_table';
  v_table_name     text;
  v_sql            text;
BEGIN

  -- Get table name
  v_table_name := fw.f_unify_name(p_name := p_table_name);

  perform fw.f_write_log(
     p_log_type := 'SERVICE', 
     p_log_message := 'START truncate table '||v_table_name, 
     p_location    := v_location); --log function call
  v_sql := 'TRUNCATE TABLE '||v_table_name;
  EXECUTE v_sql;
  perform fw.f_write_log(
     p_log_type := 'SERVICE', 
     p_log_message := 'END truncate table '||v_table_name, 
     p_location    := v_location); --log function call

END



$$
EXECUTE ON ANY;
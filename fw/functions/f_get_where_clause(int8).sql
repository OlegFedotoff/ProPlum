-- DROP FUNCTION fw.f_get_where_clause(int8);

CREATE OR REPLACE FUNCTION fw.f_get_where_clause(p_object_id int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function get where clause from object settings*/
DECLARE
  v_location text := 'fw.f_get_where_clause';
  v_where text;
BEGIN
 perform fw.f_write_log(p_log_type    := 'SERVICE', 
                         p_log_message := 'START Get where clause for object '||p_object_id, 
                         p_location    := v_location); --log function call
 select case when ltrim(o.where_clause,' ') = '' then null::text else ltrim(o.where_clause,' ')::text end 
  into v_where
   from fw.objects o
  where o.object_id = p_object_id;
  perform fw.f_write_log(p_log_type    := 'SERVICE', 
                          p_log_message := 'END Get where clause for object '||p_object_id||', where clause is: '||coalesce(v_where,'empty'), 
                          p_location    := v_location); --log function call
  return v_where;
END;



$$
EXECUTE ON ANY;
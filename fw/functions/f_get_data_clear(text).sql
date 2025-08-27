-- DROP FUNCTION fw.f_get_data_clear(text);

CREATE OR REPLACE FUNCTION fw.f_get_data_clear(p_interval text)
	RETURNS date
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
/*Function return clear data*/
DECLARE
    v_location text := 'fw.f_get_data_clear';
    v_date date;
BEGIN   
    -- Get data clear
	select 
     case 
	     when lower(p_interval) like '%year%' then 
              (date_trunc('year', current_date)) - interval '1 mon' - p_interval::interval
	     when lower(p_interval) like '%mon%' or lower(p_interval) like '%month%' then     
              (date_trunc('mon', current_date)) - interval '1 mon' - p_interval::interval
         else null
     end
    into v_date;
    PERFORM fw.f_write_log('DEBUG', 'v_date:{'||coalesce(v_date,null)||'}', v_location);
    return v_date;
END;


$$
EXECUTE ON ANY;
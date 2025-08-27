-- DROP FUNCTION fw.f_load_object(int8);

CREATE OR REPLACE FUNCTION fw.f_load_object(p_load_id int8)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	/*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function starts load function */
DECLARE
  v_location      text := 'fw.f_load_object';
  v_function_name text; 
  v_object_id     int8;
  v_res           bool;
  v_res_func      text;
  v_table_name    text;
  v_load_after_function text;

BEGIN
 -- function get load function from fw.objects and execute it
 perform fw.f_write_log(
     p_log_type    := 'SERVICE', 
     p_log_message := 'START load object with load_id = '||p_load_id, 
     p_location    := v_location,
     p_load_id     := p_load_id); --log function call
 perform fw.f_set_session_param(
    p_param_name := 'fw.load_id', 
    p_param_value := p_load_id::text);
 select ob.object_id, lower(ob.object_name)
   from fw.objects ob  inner join 
	    fw.load_info li 
	 on ob.object_id = li.object_id    
   where li.load_id  = p_load_id
   into v_object_id, v_table_name; -- get object_id function
 v_function_name = fw.f_get_load_function(p_object_id := v_object_id);
 if v_function_name is not null then 
    perform fw.f_terminate_lock(p_table_name := v_table_name);
    v_res_func = fw.f_execute_function(
       p_function_name := v_function_name,
       p_load_id       := p_load_id);
      if v_res_func = 'true' or v_res_func = 't' then 
       v_res = true;
			select prm->'load_after_function'::text from  fw.f_get_param_list(p_object_id := v_object_id) where  grp = 'COMMON' and (prm->'load_after_function') is not null
         	into v_load_after_function;
			if v_load_after_function is not null then
				v_load_after_function = replace(fw.f_replace_variables(p_load_id := p_load_id, p_string := v_load_after_function), '"','');
				perform fw.f_write_log(
			    p_log_type    := 'SERVICE', 
			    p_log_message := 'Load after function found. START run function : '||v_load_after_function, 
			    p_location    := v_location,
			    p_load_id     := p_load_id);
				raise notice 'select %',v_load_after_function;
				execute 'select ' ||v_load_after_function;
			    perform fw.f_write_log(
			    p_log_type    := 'SERVICE', 
			    p_log_message := 'END run function : '||v_load_after_function, 
			    p_location    := v_location,
			    p_load_id     := p_load_id); 
			else
				perform fw.f_write_log(
			    p_log_type    := 'SERVICE', 
			    p_log_message := 'No load after function found.', 
			    p_location    := v_location,
			    p_load_id     := p_load_id); 
			end if;
       perform fw.f_set_load_id_success(p_load_id := p_load_id);  
      elsif v_res_func  = 'false' or v_res_func = 'f' then v_res = false;
      else 
        raise notice 'Function % end with result: %',v_function_name,v_res;
        v_res = false;
        perform fw.f_set_load_id_error(p_load_id := p_load_id);  
       end if;
  else 
   v_res = false; --no function found
   perform fw.f_set_load_id_error(p_load_id := p_load_id);  
  end if;

 perform fw.f_write_log(
   p_log_type    := 'SERVICE', 
   p_log_message := 'END load object with load_id = '||p_load_id||', result is : '||coalesce(v_res_func,'empty'), 
   p_location    := v_location,
   p_load_id     := p_load_id); --log function call
 return v_res;
 exception when others then 
  raise notice 'ERROR Load object with load_id % finished with error: %',p_load_id,SQLERRM;
  PERFORM fw.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Load object with load_id = '||p_load_id||' finished with error: '||SQLERRM, 
     p_location    := v_location,
     p_load_id     := p_load_id);
   perform fw.f_set_load_id_error(p_load_id := p_load_id);  
   return false;
END;



$$
EXECUTE ON ANY;
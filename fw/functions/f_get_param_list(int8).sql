-- DROP FUNCTION fw.f_get_param_list(int8);

CREATE OR REPLACE FUNCTION fw.f_get_param_list(p_object_id int8)
	RETURNS TABLE (object_id int8, grp text, prm jsonb)
	LANGUAGE sql
	VOLATILE
AS $$
	
/*Function returns object's parameters in table form*/	
	
	SELECT o.object_id as object_id, d.KEY AS grp, jsonb_array_elements(d.value) AS prm
	FROM fw.objects o, jsonb_each(o.param_list) AS d
	WHERE o.object_id = p_object_id;



$$
EXECUTE ON ANY;
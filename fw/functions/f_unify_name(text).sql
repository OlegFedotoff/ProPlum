-- DROP FUNCTION fw.f_unify_name(text);

CREATE OR REPLACE FUNCTION fw.f_unify_name(p_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function unifies table name, column name and other names*/
DECLARE
BEGIN
  RETURN lower(trim(translate(p_name, ';/','')));
END;




$$
EXECUTE ON ANY;
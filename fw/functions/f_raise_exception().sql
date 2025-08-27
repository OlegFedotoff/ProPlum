-- DROP FUNCTION fw.f_raise_exception();

CREATE OR REPLACE FUNCTION fw.f_raise_exception()
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    v_location   text := 'fw.f_raise_exception';
/*Function to raise exception in airflow*/
BEGIN
  return false;
END;

$$
EXECUTE ON ANY;
-- DROP FUNCTION fw.f_get_locks();

CREATE OR REPLACE FUNCTION fw.f_get_locks()
	RETURNS SETOF pg_locks
	LANGUAGE sql
	SECURITY DEFINER
	VOLATILE
AS $$
	
  SELECT * FROM pg_catalog.pg_locks; 

$$
EXECUTE ON ANY;
-- DROP FUNCTION fw.f_stat_activity();

CREATE OR REPLACE FUNCTION fw.f_stat_activity()
	RETURNS SETOF pg_stat_activity
	LANGUAGE sql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	 SELECT * FROM pg_catalog.pg_stat_activity; 

$$
EXECUTE ON ANY;
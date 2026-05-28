-- DROP FUNCTION fw.f_analyze_table_sd(text);

CREATE OR REPLACE FUNCTION fw.f_analyze_table_sd(p_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
	
	
	/*Oleg Tretyakov 2026*/
/*collect statistic on table with SECURITY DEFINER*/
BEGIN

  PERFORM fw.f_analyze_table(p_table_name := p_table_name);

END


$$
EXECUTE ON ANY;

-- Необходимо выдать эти права самостоятельно после миграции
-- ALTER FUNCTION fw.f_analyze_table_sd(text) OWNER TO komus_dba;

GRANT EXECUTE ON FUNCTION fw.f_analyze_table_sd(text) TO role_data_loader;

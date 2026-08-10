-- DROP FUNCTION fw.f_terminate_lock(text);

CREATE OR REPLACE FUNCTION fw.f_terminate_lock(p_table_name text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
BEGIN
  RETURN fw.f_terminate_lock_with_source(
    p_table_name,
    'fw.f_terminate_lock'
  );
END;
$$
EXECUTE ON ANY;
CREATE OR REPLACE FUNCTION fw.f_get_table_count(table_name text)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    cnt bigint;
BEGIN
    EXECUTE 'SELECT COALESCE(sum(reltuples), 0)::bigint as total_rows
             FROM pg_class 
             JOIN pg_namespace ON relnamespace = pg_namespace.oid
             WHERE relkind = ''r''
             AND nspname||''.''||relname LIKE ' || quote_literal(table_name || '%') INTO cnt;
    RETURN cnt;
END;

$$
EXECUTE ON ANY;


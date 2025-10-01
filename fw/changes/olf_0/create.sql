DROP FUNCTION IF EXISTS fw.f_get_table_count(text);
DROP FUNCTION IF EXISTS fw.f_process_date_string(text);

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

CREATE OR REPLACE FUNCTION fw.f_process_date_string(input_string text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    result_string TEXT := input_string;
    matches TEXT[];
    days_back INTEGER;
    replacement_date TEXT;
BEGIN
    -- Обрабатываем все вхождения current_date - N
    LOOP
        -- Ищем паттерн current_date - число
        SELECT array_agg(m[1]) INTO matches 
        FROM regexp_matches(result_string, 'current_date\s*-\s*(\d+)', 'g') AS m;
        
        EXIT WHEN matches IS NULL OR array_length(matches, 1) = 0;
        
        -- Обрабатываем первое найденное вхождение
        days_back := CAST(matches[1] AS INTEGER);
        replacement_date := '''' || TO_CHAR(CURRENT_DATE - days_back, 'YYYYMMDD') || '''';
        
        -- Заменяем первое вхождение
        result_string := REGEXP_REPLACE(
            result_string,
            'current_date\s*-\s*' || days_back::TEXT,
            replacement_date,
            ''  -- заменяем только первое вхождение
        );
    END LOOP;
    
    -- Заменяем оставшиеся current_date
    result_string := REGEXP_REPLACE(
        result_string,
        'current_date',
        '''' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '''',
        'g'
    );
    
    RETURN result_string;
END;

$$
EXECUTE ON ANY;

CREATE OR REPLACE FUNCTION fw.f_get_table_count(table_name text)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
DECLARE
    cnt bigint;
    schema_name text;
    table_only text;
BEGIN
    -- Разделяем схему и имя таблицы
    SELECT split_part(table_name, '.', 1), split_part(table_name, '.', 2) 
    INTO schema_name, table_only;
    
    -- Если схема не указана, используем имя как есть
    IF table_only = '' THEN
        table_only := schema_name;
        schema_name := NULL;
    END IF;
    
    -- Считаем строки в родительской таблице и всех её партициях
    SELECT COALESCE(sum(c.reltuples), 0)::bigint INTO cnt
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relkind IN ('r', 'p')  -- обычная таблица или партиционированная таблица
    AND (
        -- Точное совпадение основной таблицы
        (n.nspname = COALESCE(schema_name, n.nspname) AND c.relname = table_only)
        OR
        -- Или это партиция этой таблицы
        c.oid IN (
            SELECT i.inhrelid 
            FROM pg_inherits i
            JOIN pg_class pc ON i.inhparent = pc.oid
            JOIN pg_namespace pn ON pc.relnamespace = pn.oid
            WHERE pn.nspname = COALESCE(schema_name, pn.nspname) 
            AND pc.relname = table_only
        )
    );
    
    RETURN cnt;
END;


$$
EXECUTE ON ANY;
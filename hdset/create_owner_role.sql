DO $$
DECLARE
    schema_name text := 'tovset';  -- ������� ������ ����� �����
    role_name text := 'role_' || schema_name || '_owner';
    role_exists INT;
    sql TEXT;
    obj RECORD;
BEGIN
    -- ���������, ���������� �� ����
    SELECT COUNT(*) INTO role_exists FROM pg_roles WHERE rolname = role_name;

    IF role_exists = 0 THEN
        -- ������ ����, ���� �� ����������
        sql := format('CREATE ROLE %I;', role_name);
        EXECUTE sql;
    END IF;

    -- ����� ����� CREATE � USAGE �� �����
    sql := format('GRANT CREATE, USAGE ON SCHEMA %I TO %I;', schema_name, role_name);
    EXECUTE sql;

    -- ��������� ����� ���� role_ml_owner
    sql := format('GRANT %I TO role_ml_owner;', role_name);
    EXECUTE sql;

    -- ����� �� �������� ������������� �� ������� ������ ����
    sql := format('GRANT role_ml_ro TO %I WITH ADMIN OPTION', role_name);
    EXECUTE sql;

    -- �������, �������������, ������������������, ������������ �������������, ������� �������
    FOR obj IN
        SELECT c.relkind,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(c.relname) AS objname,
               r.rolname AS owner,
               CASE c.relkind
                   WHEN 'r' THEN 'TABLE'
                   WHEN 'v' THEN 'VIEW'
                   WHEN 'S' THEN 'SEQUENCE'
                   WHEN 'm' THEN 'MATERIALIZED VIEW'
                   WHEN 'f' THEN 'FOREIGN TABLE'
               END AS objtype
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_roles r ON r.oid = c.relowner
        WHERE n.nspname = schema_name
          AND r.rolname IN ('komus_dba', 'komus_devops')
          AND c.relkind IN ('r','v','S','m','f')
          -- Exclude partition children
          AND NOT EXISTS (
              SELECT 1 FROM pg_inherits i
              WHERE i.inhrelid = c.oid
          )
    LOOP
        sql := format('ALTER %s %s.%s OWNER TO %I;',
                      obj.objtype, obj.schemaname, obj.objname, role_name);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    -- �������
    FOR obj IN
        SELECT p.oid,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(p.proname) AS objname,
               pg_get_function_identity_arguments(p.oid) AS args,
               r.rolname AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_roles r ON r.oid = p.proowner
        WHERE n.nspname = schema_name
          AND r.rolname IN ('komus_dba', 'komus_devops')
    LOOP
        sql := format('ALTER FUNCTION %s.%s(%s) OWNER TO %I;',
                      obj.schemaname, obj.objname, obj.args, role_name);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    -- ����
    FOR obj IN
        SELECT t.oid,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(t.typname) AS objname,
               r.rolname AS owner
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_roles r ON r.oid = t.typowner
        WHERE n.nspname = schema_name
          AND r.rolname IN ('komus_dba', 'komus_devops')
          AND t.typtype = 'c'
    LOOP
        sql := format('ALTER TYPE %s.%s OWNER TO %I;',
                      obj.schemaname, obj.objname, role_name);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

END
$$;











DO $$
DECLARE
    schema_name text := 'stg_kdw';  -- ������� ������ ����� �����
    role_name text := 'role_kdw_owner';
    role_exists INT;
    sql TEXT;
    obj RECORD;
BEGIN
    -- ���������, ���������� �� ����
    SELECT COUNT(*) INTO role_exists FROM pg_roles WHERE rolname = role_name;

    IF role_exists = 0 THEN
        -- ������ ����, ���� �� ����������
        sql := format('CREATE ROLE %I;', role_name);
        EXECUTE sql;
    END IF;

    -- ����� ����� CREATE � USAGE �� �����
    sql := format('GRANT CREATE, USAGE ON SCHEMA %I TO %I;', schema_name, role_name);
    EXECUTE sql;

    -- ��������� ����� ���� role_ml_owner
    sql := format('GRANT %I TO role_ml_owner;', role_name);
    EXECUTE sql;

    -- �������, �������������, ������������������, ������������ �������������, ������� �������
    FOR obj IN
        SELECT c.relkind,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(c.relname) AS objname,
               r.rolname AS owner,
               CASE c.relkind
                   WHEN 'r' THEN 'TABLE'
                   WHEN 'v' THEN 'VIEW'
                   WHEN 'S' THEN 'SEQUENCE'
                   WHEN 'm' THEN 'MATERIALIZED VIEW'
                   WHEN 'f' THEN 'FOREIGN TABLE'
               END AS objtype
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_roles r ON r.oid = c.relowner
        WHERE n.nspname = schema_name
          AND r.rolname IN ('komus_dba', 'komus_devops')
          AND c.relkind IN ('r','v','S','m','f')
          -- Exclude partition children
          AND NOT EXISTS (
              SELECT 1 FROM pg_inherits i
              WHERE i.inhrelid = c.oid
          )
    LOOP
        sql := format('ALTER %s %s.%s OWNER TO %I;',
                      obj.objtype, obj.schemaname, obj.objname, role_name);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    -- �������
    FOR obj IN
        SELECT p.oid,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(p.proname) AS objname,
               pg_get_function_identity_arguments(p.oid) AS args,
               r.rolname AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_roles r ON r.oid = p.proowner
        WHERE n.nspname = schema_name
          AND r.rolname IN ('komus_dba', 'komus_devops')
    LOOP
        sql := format('ALTER FUNCTION %s.%s(%s) OWNER TO %I;',
                      obj.schemaname, obj.objname, obj.args, role_name);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    -- ����
    FOR obj IN
        SELECT t.oid,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(t.typname) AS objname,
               r.rolname AS owner
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_roles r ON r.oid = t.typowner
        WHERE n.nspname = schema_name
          AND r.rolname IN ('komus_dba', 'komus_devops')
          AND t.typtype = 'c'
    LOOP
        sql := format('ALTER TYPE %s.%s OWNER TO %I;',
                      obj.schemaname, obj.objname, role_name);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

END
$$;





--�������������� ��������� �����
DO $$
DECLARE
    schema_name text := 'do_mosaic';  -- ������� ������ ����� �����
    role_name text := 'role_' || schema_name || '_owner';
    old_role text := 'komus_dba';
    
    role_exists INT;
    sql TEXT;
    obj RECORD;
BEGIN


    SELECT COUNT(*) INTO role_exists FROM pg_roles WHERE rolname = role_name;

    IF role_exists = 1 THEN
        -- ������ ����, ���� �� ����������
        sql := format('REVOKE CREATE, USAGE ON SCHEMA %I FROM %I;', schema_name, role_name);
        EXECUTE sql;
    END IF;


    -- �������, �������������, ������������������, ������������ �������������, ������� �������
    FOR obj IN
        SELECT c.relkind,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(c.relname) AS objname,
               r.rolname AS owner,
               CASE c.relkind
                   WHEN 'r' THEN 'TABLE'
                   WHEN 'v' THEN 'VIEW'
                   WHEN 'S' THEN 'SEQUENCE'
                   WHEN 'm' THEN 'MATERIALIZED VIEW'
                   WHEN 'f' THEN 'FOREIGN TABLE'
               END AS objtype
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_roles r ON r.oid = c.relowner
        WHERE n.nspname = schema_name
          AND r.rolname IN (role_name)
          AND c.relkind IN ('r','v','S','m','f')
          -- Exclude partition children
          AND NOT EXISTS (
              SELECT 1 FROM pg_inherits i
              WHERE i.inhrelid = c.oid
          )
    LOOP
        sql := format('ALTER %s %s.%s OWNER TO %I;',
                      obj.objtype, obj.schemaname, obj.objname, old_role);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    -- �������
    FOR obj IN
        SELECT p.oid,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(p.proname) AS objname,
               pg_get_function_identity_arguments(p.oid) AS args,
               r.rolname AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_roles r ON r.oid = p.proowner
        WHERE n.nspname = schema_name
          AND r.rolname IN (role_name)
    LOOP
        sql := format('ALTER FUNCTION %s.%s(%s) OWNER TO %I;',
                      obj.schemaname, obj.objname, obj.args, old_role);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    -- ����
    FOR obj IN
        SELECT t.oid,
               quote_ident(n.nspname) AS schemaname,
               quote_ident(t.typname) AS objname,
               r.rolname AS owner
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_roles r ON r.oid = t.typowner
        WHERE n.nspname = schema_name
          AND r.rolname IN (role_name)
          AND t.typtype = 'c'
    LOOP
        sql := format('ALTER TYPE %s.%s OWNER TO %I;',
                      obj.schemaname, obj.objname, old_role);
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;


    -- ���������, ���������� �� ����
    SELECT COUNT(*) INTO role_exists FROM pg_roles WHERE rolname = role_name;

    IF role_exists = 1 THEN
        -- ������ ����, ���� �� ����������
        sql := format('DROP ROLE %I;', role_name);
        EXECUTE sql;
    END IF;



END
$$;



-- �������������� ���� role_ml_ro ���� �����, ��������������� �� _owner, ����� role_ml_owner
DO $$
DECLARE
    owner_role RECORD;
    sql TEXT;
BEGIN
    FOR owner_role IN
        SELECT rolname
        FROM pg_roles
        WHERE rolname LIKE '%_owner'
          AND rolname != 'role_ml_owner'
    LOOP
        sql := format('GRANT role_ml_ro TO %I WITH ADMIN OPTION;', owner_role.rolname);
        RAISE NOTICE '%s', sql;
        EXECUTE sql;
    END LOOP;
END
$$;






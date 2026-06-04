-- DROP FUNCTION fw.f_terminate_lock(text);

CREATE OR REPLACE FUNCTION fw.f_terminate_lock(p_table_name text)
	RETURNS bool
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
	
DECLARE
  v_location       text    := 'fw.f_terminate_lock';
  v_res            bool    := false;
  v_seg_res        bool;
  v_table_name     text;
  v_root_oid       oid;
  rec              record;
BEGIN
  v_table_name := fw.f_unify_name(p_table_name);

  -- Получаем OID
  BEGIN
    v_root_oid := v_table_name::regclass::oid;
  EXCEPTION WHEN OTHERS THEN
    PERFORM fw.f_write_log(
      p_log_type    := 'ERROR',
      p_log_message := 'Cannot resolve OID for ' || v_table_name || ': ' || sqlerrm,
      p_location    := v_location
    );
    RETURN false;
  END;

  FOR rec IN (
    SELECT DISTINCT a.pid, a.sess_id
    FROM   fw.f_get_locks()      l
    JOIN   pg_catalog.pg_database d ON d.oid = l.database
    JOIN   fw.f_stat_activity()   a ON l.mppsessionid = a.sess_id
    WHERE  l.locktype   = 'relation'
      AND  d.datname    = current_database()
      AND  a.pid       <> pg_backend_pid()
      AND  a.rsgname   <> 'etl_group'
      AND  l.relation  IN (

        SELECT v_root_oid

        UNION
        
        -- 2. Всё дерево потомков через pg_inherits покрывает все партиции
        SELECT inhrelid
        FROM (
          WITH RECURSIVE inh AS (
            SELECT inhrelid
            FROM   pg_catalog.pg_inherits
            WHERE  inhparent = v_root_oid

            UNION ALL

            SELECT i.inhrelid
            FROM   pg_catalog.pg_inherits i
            JOIN   inh ON inh.inhrelid = i.inhparent
          )
          SELECT inhrelid FROM inh
        ) t

        UNION

        SELECT (partitionschemaname || '.' || partitiontablename)::regclass
        FROM   pg_catalog.pg_partitions
        WHERE  schemaname = split_part(v_table_name, '.', 1)
          AND  tablename  = split_part(v_table_name, '.', 2)

      )
  )
  LOOP
    -- ШАГ 1: Завершаем QD-процесс на мастере
    v_res := fw.f_terminate_backend(rec.pid);

    -- ШАГ 2: Завершаем QE-процессы на всех сегментах по sess_id
    BEGIN
      SELECT bool_or(pg_terminate_backend(pid))
      INTO   v_seg_res
      FROM   gp_dist_random('pg_stat_activity')
      WHERE  sess_id = rec.sess_id;   -- sess_id одинаков на мастере и сегментах
    EXCEPTION WHEN OTHERS THEN
      v_seg_res := false;
      PERFORM fw.f_write_log(
        p_log_type    := 'ERROR',
        p_log_message := 'Segment terminate failed for sess_id=' || rec.sess_id
                         || ': ' || sqlerrm,
        p_location    := v_location
      );
    END;

    PERFORM fw.f_write_log(
      p_log_type    := 'SERVICE',
      p_log_message := 'Terminate lock: pid=' || rec.pid
                       || ' sess_id=' || rec.sess_id
                       || ' master=' || coalesce(v_res::text, 'null')
                       || ' segments=' || coalesce(v_seg_res::text, 'null'),
      p_location    := v_location
    );
  END LOOP;

  PERFORM fw.f_write_log(
    p_log_type    := 'SERVICE',
    p_log_message := 'Terminate locks on object ' || v_table_name || ' completed',
    p_location    := v_location
  );

  RETURN coalesce(v_res, false);

EXCEPTION WHEN OTHERS THEN
  PERFORM fw.f_write_log(
    p_log_type    := 'ERROR',
    p_log_message := 'Terminate locks on object ' || v_table_name
                     || ' finished with error: ' || sqlerrm,
    p_location    := v_location
  );
  RETURN false;
END;


$$
EXECUTE ON ANY;

-- Необходимо выдать эти права самостоятельно после миграции
-- ALTER FUNCTION fw.f_terminate_lock(text) OWNER TO komus_dba;

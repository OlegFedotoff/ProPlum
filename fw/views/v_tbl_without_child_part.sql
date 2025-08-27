-- fw.v_tbl_without_child_part исходный текст

CREATE OR REPLACE VIEW fw.v_tbl_without_child_part
AS SELECT o.schemaname,
    o.tablename,
    o.tableowner,
    o.tabletype,
    o.statime
   FROM ( SELECT t1.schemaname,
            t1.tablename,
            t1.tableowner,
                CASE
                    WHEN t3.tablename IS NULL AND t2.tablename IS NULL THEN 'r'::text
                    WHEN t3.tablename IS NOT NULL AND t2.tablename IS NULL THEN 'p-root'::text
                    ELSE 'p-child'::text
                END AS tabletype,
            t4.statime
           FROM pg_tables t1
             LEFT JOIN pg_partitions t2 ON t1.tablename = t2.partitiontablename AND t1.schemaname = t2.partitionschemaname
             LEFT JOIN ( SELECT pg_partitions.schemaname,
                    pg_partitions.tablename
                   FROM pg_partitions
                  WHERE pg_partitions.schemaname ~~ '%kdw%'::text OR pg_partitions.schemaname ~~ '%hybris%'::text OR pg_partitions.schemaname ~~ '%pricepoint%'::text OR pg_partitions.schemaname ~~ '%swan%'::text OR pg_partitions.schemaname ~~ '%etl%'::text OR pg_partitions.schemaname ~~ '%fw%'::text
                  GROUP BY pg_partitions.schemaname, pg_partitions.tablename) t3 ON t1.tablename = t3.tablename AND t1.schemaname = t3.schemaname
             LEFT JOIN ( SELECT n.schemaname,
                    n.objname,
                    n.actionname,
                    n.statime,
                    n.rn
                   FROM ( SELECT pg_stat_operations.schemaname,
                            pg_stat_operations.objname,
                            pg_stat_operations.actionname,
                            pg_stat_operations.statime,
                            row_number() OVER (PARTITION BY pg_stat_operations.schemaname, pg_stat_operations.objname ORDER BY pg_stat_operations.statime DESC) AS rn
                           FROM pg_stat_operations
                          WHERE (pg_stat_operations.classname = 'pg_class'::text AND pg_stat_operations.schemaname ~~ '%kdw%'::text) OR pg_stat_operations.schemaname ~~ '%hybris%'::text OR pg_stat_operations.schemaname ~~ '%pricepoint%'::text OR pg_stat_operations.schemaname ~~ '%swan%'::text OR pg_stat_operations.schemaname ~~ '%etl%'::text OR (pg_stat_operations.schemaname ~~ '%fw%'::text AND (pg_stat_operations.actionname = ANY (ARRAY['CREATE'::name, 'ALTER'::name])))) n
                  WHERE n.rn = 1) t4 ON t1.schemaname = t4.schemaname AND t1.tablename = t4.objname
          WHERE t1.schemaname ~~ '%kdw%'::text OR t1.schemaname ~~ '%hybris%'::text OR t1.schemaname ~~ '%pricepoint%'::text OR t1.schemaname ~~ '%swan%'::text OR t1.schemaname ~~ '%etl%'::text OR t1.schemaname ~~ '%fw%'::text) o
  WHERE o.tabletype = ANY (ARRAY['r'::text, 'p-root'::text]);



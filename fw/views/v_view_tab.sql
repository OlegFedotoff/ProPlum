-- fw.v_view_tab исходный текст

CREATE OR REPLACE VIEW fw.v_view_tab
AS SELECT t1.nspname AS schemaname,
    p.relname AS tablename,
    p.relkind AS tab_view
   FROM pg_class p
     LEFT JOIN pg_namespace t1 ON p.relnamespace = t1.oid
  WHERE (p.relkind = ANY (ARRAY['p'::"char", 'r'::"char", 'v'::"char"])) AND (t1.nspname ~~ '%kdw%'::text OR t1.nspname ~~ '%hybris%'::text OR t1.nspname ~~ '%pricepoint%'::text OR t1.nspname ~~ '%swan%'::text OR t1.nspname ~~ '%etl%'::text);
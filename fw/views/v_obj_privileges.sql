-- fw.v_obj_privileges исходный текст

CREATE OR REPLACE VIEW fw.v_obj_privileges
AS SELECT b.obj_name,
    b.obj_type,
    b.grantee,
    b.grantor,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%r%'::text THEN true
                ELSE false
            END
        END AS select_priv,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%a%'::text THEN true
                ELSE false
            END
        END AS insert_priv,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%w%'::text THEN true
                ELSE false
            END
        END AS update_priv,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%d%'::text THEN true
                ELSE false
            END
        END AS delete_priv,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%x%'::text THEN true
                ELSE false
            END
        END AS reference_priv,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%t%'::text THEN true
                ELSE false
            END
        END AS trigger_priv,
        CASE
            WHEN b.grantee IS NULL THEN NULL::boolean
            ELSE
            CASE
                WHEN b.acc ~~ '%X%'::text THEN true
                ELSE false
            END
        END AS execute_priv
   FROM ( SELECT a.obj_name,
            a.obj_type,
            split_part(a.priv, '='::text, 1) AS grantee,
            split_part(split_part(a.priv, '='::text, 2), '/'::text, 2) AS grantor,
            split_part(split_part(a.priv, '='::text, 2), '/'::text, 1) AS acc
           FROM ( SELECT (n.nspname::text || '.'::text) || c.relname::text AS obj_name,
                        CASE c.relkind
                            WHEN 'r'::"char" THEN
                            CASE
                                WHEN c.relstorage = 'x'::"char" THEN 'external table'::text
                                ELSE 'table'::text
                            END
                            WHEN 'v'::"char" THEN 'view'::text
                            WHEN 'S'::"char" THEN 'sequence'::text
                            ELSE NULL::text
                        END AS obj_type,
                    privs.priv
                   FROM pg_class c
                     LEFT JOIN ( SELECT pg_class.oid,
                            regexp_split_to_table(array_to_string(pg_class.relacl, ','::text), ','::text) AS priv
                           FROM pg_class) privs ON privs.oid = c.oid
                     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'S'::"char"])) AND (n.nspname <> ALL (ARRAY['gp_toolkit'::name, 'information_schema'::name, 'pg_catalog'::name, 'pg_aoseg'::name, 'pg_toast'::name]))) a) b;



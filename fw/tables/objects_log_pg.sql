all: |
    DROP FOREIGN TABLE fw.objects_log_pg cascade;
dev: |
    CREATE FOREIGN TABLE fw.objects_log_pg 
    ( object_id int8 NOT NULL, object_name text NOT NULL, object_desc text NULL, 
    extraction_type text NULL, load_type text NULL, merge_key _text NULL, 
    delta_field text NULL, delta_field_format text NULL, delta_safety_period interval DEFAULT '00:00:00'::interval NULL,
    bdate_field_format text NULL, bdate_safety_period interval DEFAULT '00:00:00'::interval NULL, load_method text NULL, 
    job_name text NULL, responsible_mail _text NULL, priority int4 NULL, 
    periodicity interval NULL, load_interval interval NULL, activitystart time NULL, 
    activityend time NULL, "active" bool DEFAULT true NULL, 
    load_start_date timestamp DEFAULT '2000-01-01 00:00:00'::timestamp without time zone NULL, 
    delta_start_date timestamp DEFAULT '2000-01-01 00:00:00'::timestamp without time zone NULL, 
    delta_mode text NULL, connect_string text NULL, load_function_name text NULL, 
    where_clause text NULL, load_group text NULL, src_date_type text NULL, 
    src_ts_type text NULL, column_name_mapping jsonb NULL, transform_mapping jsonb NULL, 
    delta_field_type text NULL, bdate_field_type text NULL, change_type text NULL, 
    change_timestamp timestamp NULL, change_username text NULL
    )
    SERVER pg_fw_dev
    OPTIONS (table_name 'objects_log');

prod: |
    CREATE FOREIGN TABLE fw.objects_log_pg 
    ( object_id int8 NOT NULL, object_name text NOT NULL, object_desc text NULL, 
    extraction_type text NULL, load_type text NULL, merge_key _text NULL, 
    delta_field text NULL, delta_field_format text NULL, delta_safety_period interval DEFAULT '00:00:00'::interval NULL,
    bdate_field_format text NULL, bdate_safety_period interval DEFAULT '00:00:00'::interval NULL, load_method text NULL, 
    job_name text NULL, responsible_mail _text NULL, priority int4 NULL, 
    periodicity interval NULL, load_interval interval NULL, activitystart time NULL, 
    activityend time NULL, "active" bool DEFAULT true NULL, 
    load_start_date timestamp DEFAULT '2000-01-01 00:00:00'::timestamp without time zone NULL, 
    delta_start_date timestamp DEFAULT '2000-01-01 00:00:00'::timestamp without time zone NULL, 
    delta_mode text NULL, connect_string text NULL, load_function_name text NULL, 
    where_clause text NULL, load_group text NULL, src_date_type text NULL, 
    src_ts_type text NULL, column_name_mapping jsonb NULL, transform_mapping jsonb NULL, 
    delta_field_type text NULL, bdate_field_type text NULL, change_type text NULL, 
    change_timestamp timestamp NULL, change_username text NULL
    )
    SERVER pg_fw_prod
    OPTIONS (table_name 'objects_log');
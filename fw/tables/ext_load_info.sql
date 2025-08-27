-- fw.ext_load_info определение

-- Drop table

-- DROP EXTERNAL TABLE fw.ext_load_info;

CREATE EXTERNAL TABLE ADWH.fw.ext_load_info (
	load_type int8,
	load_name text,
	last_date timestamp,
	load_date timestamp,
	last_key int8,
	load_days int8,
	load_group int8,
	load_status int8,
	priority int8,
	proc_name text,
	proc_type int8,
	queue_group1 text,
	queue_group2 text,
	table_src text,
	table_dst text,
	temp_table text,
	auth_id int8,
	auth_col text,
	time_updated_col text,
	key_columns text,
	old_table_dst text,
	id_column text,
	seq_name text,
	group_type text,
	is_group_end text,
	disabled text,
	flag_reload text,
	reload_date date,
	dim_master_data text,
	dim_parent_load_type text
)
LOCATION (
	'pxf://KDW.V_LOAD_INFO?PROFILE=Jdbc&server=kdw'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

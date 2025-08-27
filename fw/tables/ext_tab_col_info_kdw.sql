-- fw.ext_tab_col_info_kdw определение

-- Drop table

-- DROP EXTERNAL TABLE fw.ext_tab_col_info_kdw;

CREATE EXTERNAL TABLE ADWH.fw.ext_tab_col_info_kdw (
	owner text,
	table_name text,
	column_name text,
	data_type text,
	data_length text,
	data_precision text,
	data_scale text,
	nullable text,
	column_id int8,
	num_distinct int8,
	low_value text,
	high_value text,
	density text
)
LOCATION (
	'pxf://ALL_TAB_COLUMNS?PROFILE=Jdbc&server=kdw'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

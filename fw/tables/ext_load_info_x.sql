-- fw.ext_load_info_x определение

-- Drop table

-- DROP EXTERNAL TABLE fw.ext_load_info_x;

CREATE EXTERNAL TABLE ADWH.fw.ext_load_info_x (
	load_date timestamp,
	group_type text
)
LOCATION (
	'pxf://KDW.V_LOAD_INFO_GP?PROFILE=Jdbc&server=kdw'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

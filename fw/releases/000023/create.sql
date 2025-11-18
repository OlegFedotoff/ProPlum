CREATE TABLE fw.odata_query_helper (
	load_id int8 NOT NULL,
	sql_query text NOT NULL,
	table_to text NOT NULL,
	rows_count int8 NOT NULL,
	delta_field text NULL,
	extraction_to timestamp NULL
)
DISTRIBUTED BY (load_id);

INSERT INTO fw.d_load_method (load_method, desc_short, desc_middle, desc_long) 
VALUES('zservice_async', 'Загрузка через zservice в асинхронном режиме', 'Load from zservice async', NULL);

  




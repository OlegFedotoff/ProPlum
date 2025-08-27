-- fw.hdset_params определение

-- Drop table

-- DROP TABLE fw.hdset_params;

CREATE TABLE fw.hdset_params (
	code varchar(255) NOT NULL, -- Код
	value varchar(255) NULL, -- Значение
	CONSTRAINT hdset_params_pkey PRIMARY KEY (code)
)
DISTRIBUTED BY (code);
COMMENT ON TABLE fw.hdset_params IS 'Параметры репзитария';

-- Column comments

COMMENT ON COLUMN fw.hdset_params.code IS 'Код';
COMMENT ON COLUMN fw.hdset_params.value IS 'Значение';



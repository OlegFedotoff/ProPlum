-- fw.locks определение

-- Drop table

-- DROP TABLE fw.locks;

CREATE TABLE fw.locks (
	load_id int8 NULL, -- load_id из load_info
	pid int4 NULL, -- pid процесса, установившего блокировку
	lock_type text NULL, -- Тип блокировки
	object_name text NULL, -- Загружаемый объект
	lock_timestamp timestamp DEFAULT now() NULL, -- Метка времени установки блокировки
	lock_user text DEFAULT "current_user"() NULL -- Пользователь, установивший блокировку
)
DISTRIBUTED BY (load_id);
COMMENT ON TABLE fw.locks IS 'Таблица содержит в себе информацию о текущих блокировках в процессах загрузки данных';

-- Column comments

COMMENT ON COLUMN fw.locks.load_id IS 'load_id из load_info';
COMMENT ON COLUMN fw.locks.pid IS 'pid процесса, установившего блокировку';
COMMENT ON COLUMN fw.locks.lock_type IS 'Тип блокировки';
COMMENT ON COLUMN fw.locks.object_name IS 'Загружаемый объект';
COMMENT ON COLUMN fw.locks.lock_timestamp IS 'Метка времени установки блокировки';
COMMENT ON COLUMN fw.locks.lock_user IS 'Пользователь, установивший блокировку';



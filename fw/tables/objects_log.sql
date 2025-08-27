-- fw.objects_log определение

-- Drop table

-- DROP TABLE fw.objects_log;

CREATE TABLE fw.objects_log (
	object_id int8 NOT NULL, -- id объекта. Заполняется инкрементально от последнего заполненного значения в таблице. Повторяющиеся значения не допускаются
	object_name text NOT NULL, -- Имя заполняемого объекта (таблицы). Заполняется с указанием схемы
	object_desc text NULL, -- Осмысленное описание целевого объекта
	extraction_type text NULL, -- Способ экстракции данных из таблицы-источника. Принимает значения: DELTA, PARTITION, FULL
	load_type text NULL, -- Тип загрузки данных в целевой объект. Принимает значения: FULL, DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT), PARTITION, UPDATE_PARTITION
	merge_key _text NULL, -- Ключ для мерджа дельты. Заполняется для способов загрузки: DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT), DELTA_UPDATE_PARTITION
	delta_field text NULL, -- Поле для выделения дельты из исходной таблицы. Заполнение обязательно для типов загрузки: DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT)
	delta_field_format text NULL, -- Формат даты для поля delta_field.
	delta_safety_period interval DEFAULT '00:00:00'::interval NULL, -- Доверительный интервал для поля delta_field. Период для учитывания данных прошлых периодов при формировании load_start для load_id. Заполнение релевантно для типов загрузки DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT)
	bdate_field text NULL, -- Поле для бизнес даты. Обязательно для типа загрузки DELTA_PARTITION
	bdate_field_format text NULL, -- Формат даты для поля bdate_field
	bdate_safety_period interval DEFAULT '00:00:00'::interval NULL, -- Доверительный интервал для bdate_field. Период для учитывания данных прошлых периодов при при формировании load_start для load_id. Заполнение релевантно для типа загрузки DELTA_PARTITION
	load_method text NULL, -- Метод загрузки - способ получения данных для целевой таблицы. Заполнение обязательно. Возможные значения: pxf, gpfdist, function, dblink
	job_name text NULL, -- Имя задания Airflow, загружающего данный объект
	responsible_mail _text NULL, -- Список почтовых адресов, ответственных за загрузку объекта
	priority int4 NULL, -- Приоритет загрузки объекта. Используется при построении зависимостей
	periodicity interval NULL, -- Периодичность запуска расчета для объекта. Информационное поле
	load_interval interval NULL, -- Период формирования и округления extraction_start - extraction_end для load_id. Заполнение обязательно
	activitystart time NULL, -- Старт времени активности работы загрузки для объекта. Информационное поле
	activityend time NULL, -- Окончание времени активности работы загрузки для объекта. Информационное поле
	"active" bool DEFAULT true NULL, -- Флаг активности объекта. Принимает значения: true, false
	load_start_date timestamp DEFAULT '2000-01-01 00:00:00'::timestamp without time zone NULL, -- Начальная дата загрузки данных по bdate_field. Заполнение релевантно для типа загрузки DELTA_PARTITION
	delta_start_date timestamp DEFAULT '2000-01-01 00:00:00'::timestamp without time zone NULL, -- Начальная дата загрузки дельты по delta_field. Заполнение релевантно для типов загрузки: DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT)
	delta_mode text NULL, -- Режим дельты при формировании load_id: DELTA (один load_id), FULL (один load_id), ITER (несколько load_id с периодичностью load_interval). Принимает значения: DELTA, FULL, ITER
	connect_string text NULL, -- Строка подключения к источнику. Для pxf - cодержит в себе имя таблицы-источника в исходной базе, профиль pxf-подключения и сервер; для gpfdist - имя файла, порт
	load_function_name text NULL, -- Функция по расчету данных после загрузки / функция расчета целевой таблицы. Возможно использование переменных: $load_from, $load_to, $delta_from, $delta_to, $load_id, $load_type, $object_id, значения которых определяются для конкретного load_id
	where_clause text NULL, -- Условие, применяемое при загрузке данных из исходной системы. Если оно указано, то игнорируются условия фреймворка по определению дельты. Возможно использование переменных: $load_from, $load_to, $delta_from, $delta_to, значения которых определяются для конкретного load_id
	load_group text NULL, -- Группа загрузок в Airflow, отвечает за то, в каком блоке будет производиться загрузка данных указанной таблицы
	src_date_type text NULL, -- Тип поля даты в системе-источнике. Учитывается при создании внешней таблицы, когда тип поля-источника - не дата, а поле приемника - дата
	src_ts_type text NULL, -- Тип поля метки времени (таймстемпа) в системе-источнике. Учитывается при создании внешней таблицы, когда тип поля-источника - не таймстемп, а поле приемника - таймстемп
	column_name_mapping jsonb NULL, -- Мэппинг имен полей таблицы-источника к таблице-приемнику. Используется, когда имя поля в базе-источнике не совпадает с именем поля в базе-приемнике
	transform_mapping jsonb NULL, -- Преобразование полей таблицы-источника для заполнения поля таблицы-приемника. Используется синтаксис SQL
	delta_field_type text NULL, -- Тип поля дельты в системе-источнике
	bdate_field_type text NULL, -- Тип поля бизнес даты в системе-источнике
	change_type text NULL, -- Тип изменения записи
	change_timestamp timestamp NULL, -- Метка времени изменения записи
	change_username text NULL -- Пользователь, изменивший запись
)
DISTRIBUTED BY (object_id);
COMMENT ON TABLE fw.objects_log IS 'История изменения настроек объектов';

-- Column comments

COMMENT ON COLUMN fw.objects_log.object_id IS 'id объекта. Заполняется инкрементально от последнего заполненного значения в таблице. Повторяющиеся значения не допускаются';
COMMENT ON COLUMN fw.objects_log.object_name IS 'Имя заполняемого объекта (таблицы). Заполняется с указанием схемы';
COMMENT ON COLUMN fw.objects_log.object_desc IS 'Осмысленное описание целевого объекта';
COMMENT ON COLUMN fw.objects_log.extraction_type IS 'Способ экстракции данных из таблицы-источника. Принимает значения: DELTA, PARTITION, FULL';
COMMENT ON COLUMN fw.objects_log.load_type IS 'Тип загрузки данных в целевой объект. Принимает значения: FULL, DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT), PARTITION, UPDATE_PARTITION';
COMMENT ON COLUMN fw.objects_log.merge_key IS 'Ключ для мерджа дельты. Заполняется для способов загрузки: DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT), DELTA_UPDATE_PARTITION';
COMMENT ON COLUMN fw.objects_log.delta_field IS 'Поле для выделения дельты из исходной таблицы. Заполнение обязательно для типов загрузки: DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT)';
COMMENT ON COLUMN fw.objects_log.delta_field_format IS 'Формат даты для поля delta_field.';
COMMENT ON COLUMN fw.objects_log.delta_safety_period IS 'Доверительный интервал для поля delta_field. Период для учитывания данных прошлых периодов при формировании load_start для load_id. Заполнение релевантно для типов загрузки DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT)';
COMMENT ON COLUMN fw.objects_log.bdate_field IS 'Поле для бизнес даты. Обязательно для типа загрузки DELTA_PARTITION';
COMMENT ON COLUMN fw.objects_log.bdate_field_format IS 'Формат даты для поля bdate_field';
COMMENT ON COLUMN fw.objects_log.bdate_safety_period IS 'Доверительный интервал для bdate_field. Период для учитывания данных прошлых периодов при при формировании load_start для load_id. Заполнение релевантно для типа загрузки DELTA_PARTITION';
COMMENT ON COLUMN fw.objects_log.load_method IS 'Метод загрузки - способ получения данных для целевой таблицы. Заполнение обязательно. Возможные значения: pxf, gpfdist, function, dblink';
COMMENT ON COLUMN fw.objects_log.job_name IS 'Имя задания Airflow, загружающего данный объект';
COMMENT ON COLUMN fw.objects_log.responsible_mail IS 'Список почтовых адресов, ответственных за загрузку объекта';
COMMENT ON COLUMN fw.objects_log.priority IS 'Приоритет загрузки объекта. Используется при построении зависимостей';
COMMENT ON COLUMN fw.objects_log.periodicity IS 'Периодичность запуска расчета для объекта. Информационное поле';
COMMENT ON COLUMN fw.objects_log.load_interval IS 'Период формирования и округления extraction_start - extraction_end для load_id. Заполнение обязательно';
COMMENT ON COLUMN fw.objects_log.activitystart IS 'Старт времени активности работы загрузки для объекта. Информационное поле';
COMMENT ON COLUMN fw.objects_log.activityend IS 'Окончание времени активности работы загрузки для объекта. Информационное поле';
COMMENT ON COLUMN fw.objects_log."active" IS 'Флаг активности объекта. Принимает значения: true, false';
COMMENT ON COLUMN fw.objects_log.load_start_date IS 'Начальная дата загрузки данных по bdate_field. Заполнение релевантно для типа загрузки DELTA_PARTITION';
COMMENT ON COLUMN fw.objects_log.delta_start_date IS 'Начальная дата загрузки дельты по delta_field. Заполнение релевантно для типов загрузки: DELTA, DELTA_MERGE, DELTA_UPSERT (DELETE_INSERT)';
COMMENT ON COLUMN fw.objects_log.delta_mode IS 'Режим дельты при формировании load_id: DELTA (один load_id), FULL (один load_id), ITER (несколько load_id с периодичностью load_interval). Принимает значения: DELTA, FULL, ITER';
COMMENT ON COLUMN fw.objects_log.connect_string IS 'Строка подключения к источнику. Для pxf - cодержит в себе имя таблицы-источника в исходной базе, профиль pxf-подключения и сервер; для gpfdist - имя файла, порт';
COMMENT ON COLUMN fw.objects_log.load_function_name IS 'Функция по расчету данных после загрузки / функция расчета целевой таблицы. Возможно использование переменных: $load_from, $load_to, $delta_from, $delta_to, $load_id, $load_type, $object_id, значения которых определяются для конкретного load_id';
COMMENT ON COLUMN fw.objects_log.where_clause IS 'Условие, применяемое при загрузке данных из исходной системы. Если оно указано, то игнорируются условия фреймворка по определению дельты. Возможно использование переменных: $load_from, $load_to, $delta_from, $delta_to, значения которых определяются для конкретного load_id';
COMMENT ON COLUMN fw.objects_log.load_group IS 'Группа загрузок в Airflow, отвечает за то, в каком блоке будет производиться загрузка данных указанной таблицы';
COMMENT ON COLUMN fw.objects_log.src_date_type IS 'Тип поля даты в системе-источнике. Учитывается при создании внешней таблицы, когда тип поля-источника - не дата, а поле приемника - дата';
COMMENT ON COLUMN fw.objects_log.src_ts_type IS 'Тип поля метки времени (таймстемпа) в системе-источнике. Учитывается при создании внешней таблицы, когда тип поля-источника - не таймстемп, а поле приемника - таймстемп';
COMMENT ON COLUMN fw.objects_log.column_name_mapping IS 'Мэппинг имен полей таблицы-источника к таблице-приемнику. Используется, когда имя поля в базе-источнике не совпадает с именем поля в базе-приемнике';
COMMENT ON COLUMN fw.objects_log.transform_mapping IS 'Преобразование полей таблицы-источника для заполнения поля таблицы-приемника. Используется синтаксис SQL';
COMMENT ON COLUMN fw.objects_log.delta_field_type IS 'Тип поля дельты в системе-источнике';
COMMENT ON COLUMN fw.objects_log.bdate_field_type IS 'Тип поля бизнес даты в системе-источнике';
COMMENT ON COLUMN fw.objects_log.change_type IS 'Тип изменения записи';
COMMENT ON COLUMN fw.objects_log.change_timestamp IS 'Метка времени изменения записи';
COMMENT ON COLUMN fw.objects_log.change_username IS 'Пользователь, изменивший запись';



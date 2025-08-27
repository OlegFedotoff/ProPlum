-- fw.objects_archivation определение

-- Drop table

-- DROP TABLE fw.objects_archivation;

CREATE TABLE fw.objects_archivation (
	object_id int8 NOT NULL, -- id объекта. Соответствует id объекта в fw.objects
	write_connect_string text NOT NULL, -- Коннект для таблицы на запись
	read_connect_string text NOT NULL, -- Коннект для таблицы на чтение
	repartitioning bool DEFAULT false NULL, -- Необходимо ли репартицирование с дневных партиций на месячные
	tmp_schema text NOT NULL, -- Наименование схемы для создания внешних таблиц, которые смотрят в S3. После архивирования является схемой, в которой остается физическая копия партиции (зависит от параметра delete_physical)
	delete_physical bool DEFAULT false NULL, -- Параметр показывает - нужно ли после архивирования удалять физические партиции в АХД
	period_of_physical_live_in_month numeric NOT NULL, -- Период определяет границы хранения физически данных в АХД. На уровне функций происходит округление до 1 числа месяца в большую сторону
	sort_sentence text NULL -- Предложение для сортировки данных в S3. Позволяет сэкономить в S3 место
)
DISTRIBUTED REPLICATED;
COMMENT ON TABLE fw.objects_archivation IS 'Настройки для архивации объектов в S3 (created from hdset 2025-03-13)';

-- Column comments

COMMENT ON COLUMN fw.objects_archivation.object_id IS 'id объекта. Соответствует id объекта в fw.objects';
COMMENT ON COLUMN fw.objects_archivation.write_connect_string IS 'Коннект для таблицы на запись';
COMMENT ON COLUMN fw.objects_archivation.read_connect_string IS 'Коннект для таблицы на чтение';
COMMENT ON COLUMN fw.objects_archivation.repartitioning IS 'Необходимо ли репартицирование с дневных партиций на месячные';
COMMENT ON COLUMN fw.objects_archivation.tmp_schema IS 'Наименование схемы для создания внешних таблиц, которые смотрят в S3. После архивирования является схемой, в которой остается физическая копия партиции (зависит от параметра delete_physical)';
COMMENT ON COLUMN fw.objects_archivation.delete_physical IS 'Параметр показывает - нужно ли после архивирования удалять физические партиции в АХД';
COMMENT ON COLUMN fw.objects_archivation.period_of_physical_live_in_month IS 'Период определяет границы хранения физически данных в АХД. На уровне функций происходит округление до 1 числа месяца в большую сторону';
COMMENT ON COLUMN fw.objects_archivation.sort_sentence IS 'Предложение для сортировки данных в S3. Позволяет сэкономить в S3 место';



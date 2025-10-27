ALTER TABLE fw.load_info
ALTER COLUMN created_dttm SET DEFAULT now();

ALTER TABLE fw.load_info
ALTER COLUMN updated_dttm SET DEFAULT now();

COMMENT ON COLUMN fw.load_info.created_dttm IS 'Метка времени создания load_id';
COMMENT ON COLUMN fw.load_info.updated_dttm IS 'Метка времени изменения load_id';
ALTER TABLE fw.load_info
ALTER COLUMN created_dttm SET DEFAULT (now() AT TIME ZONE 'UTC');

ALTER TABLE fw.load_info
ALTER COLUMN updated_dttm SET DEFAULT (now() AT TIME ZONE 'UTC');

COMMENT ON COLUMN fw.load_info.created_dttm IS 'Метка времени создания load_id (UTC)';
COMMENT ON COLUMN fw.load_info.updated_dttm IS 'Метка времени изменения load_id (UTC)';
DROP FUNCTION IF EXISTS fw.f_terminate_lock_with_source(text, text);

-- Необходимо выдать эти права самостоятельно до миграции
-- ALTER FUNCTION fw.f_terminate_lock_with_source(text, text) OWNER TO role_fw_owner;
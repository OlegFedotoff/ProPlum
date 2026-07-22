DO $$
BEGIN

    DELETE FROM fw.ext_tables_params
    WHERE object_id IN (13000);

END $$;

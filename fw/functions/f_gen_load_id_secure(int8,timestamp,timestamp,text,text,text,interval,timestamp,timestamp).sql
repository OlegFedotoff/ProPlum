-- Wrapper with SECURITY DEFINER for fw.f_gen_load_id

CREATE OR REPLACE FUNCTION fw.f_gen_load_id_secure(
    p_object_id int8,
    p_start_extr timestamp DEFAULT NULL::timestamp without time zone,
    p_end_extr timestamp DEFAULT NULL::timestamp without time zone,
    p_extraction_type text DEFAULT NULL::text,
    p_load_type text DEFAULT NULL::text,
    p_delta_mode text DEFAULT NULL::text,
    p_load_interval interval DEFAULT NULL::interval,
    p_start_load timestamp DEFAULT NULL::timestamp without time zone,
    p_end_load timestamp DEFAULT NULL::timestamp without time zone
)
    RETURNS int8
    LANGUAGE plpgsql
    SECURITY DEFINER
    VOLATILE
AS $$
DECLARE
    v_res int8;
BEGIN
    PERFORM set_config('search_path','fw,pg_temp',true);

    v_res := fw.f_gen_load_id(
        p_object_id      := p_object_id,
        p_start_extr     := p_start_extr,
        p_end_extr       := p_end_extr,
        p_extraction_type:= p_extraction_type,
        p_load_type      := p_load_type,
        p_delta_mode     := p_delta_mode,
        p_load_interval  := p_load_interval,
        p_start_load     := p_start_load,
        p_end_load       := p_end_load
    );
    RETURN v_res;
END;
$$
EXECUTE ON ANY;

GRANT EXECUTE ON FUNCTION fw.f_gen_load_id_secure(
    int8,timestamp,timestamp,text,text,text,interval,timestamp,timestamp
) TO role_data_loader;



all: |

dev: |


    DO $$
    BEGIN

    INSERT INTO fw.ext_tables_params (object_id, load_method, connection_string, additional, "active")
    VALUES (
        13000,
        'zservice_async',
        'location (''gpfdist://10.196.0.32:8082/komus_erp/13000_src_komus_erp_zds_fi_acdoca_10.gz'') on all FORMAT ''CSV'' (delimiter '';'' null '''' header) ENCODING ''UTF8'';',
        'SAPI/zds_fi_acdoca_10?sap-client=510&subscriber_proc=TEST&mode=F&maxpackagesize=100',
        true
    );

    END $$;
    COMMIT;

prod: |

    DO $$
    BEGIN

    INSERT INTO fw.ext_tables_params (object_id, load_method, connection_string, additional, "active")
    VALUES (
        13000,
        'zservice_async',
        'location (''gpfdist://10.196.0.32:8081/komus_erp/13000_src_komus_erp_zds_fi_acdoca_10.gz'') on all FORMAT ''CSV'' (delimiter '';'' null '''' header) ENCODING ''UTF8'';',
        'SAPI/zds_fi_acdoca_10?sap-client=510&subscriber_proc=PROD&mode=F&maxpackagesize=100',
        true
    );

    END $$;
    COMMIT;

INSERT INTO fw.dq_testcases ( testcase_id
                            , testcase_name
                            , testcase_desc
                            , object_name
                            , testcase_sql
                            , benchmark_sql
                            , test_group
                            , key_fields
                            , active
                            , connect_type
                            , object_id
                            , connect_name)
SELECT '640'
     , 'Таблица ценовых диапазонов товарных групп'
     , 'Таблица ценовых диапазонов товарных групп'
     , 'kdw.price_range_g'
     , 'select count(*) cnt from kdw.price_range_g'
     , 'select count(*) cnt from KDW.DWE_PRICE_RANGE_G_GP'
     , 'KDW'
     , NULL
     , TRUE
     , 'OracleHook'
     , '1064'
     , 'kdw';

INSERT INTO fw.dq_testcases ( testcase_id
                            , testcase_name
                            , testcase_desc
                            , object_name
                            , testcase_sql
                            , benchmark_sql
                            , test_group
                            , key_fields
                            , active
                            , connect_type
                            , object_id
                            , connect_name)
SELECT '639'
     , 'Справочник "Таблица аналогов товаров"'
     , 'Справочник "Таблица аналогов товаров"'
     , 'kdw.item_analog'
     ,'select count(*) cnt from kdw.item_analog'
     , 'select count(*) cnt from KDW.DWD_ITEM_ANALOG_GP'
     , 'KDW'
     , NULL
     , TRUE
     , 'OracleHook'
     , '1063'
     , 'kdw';
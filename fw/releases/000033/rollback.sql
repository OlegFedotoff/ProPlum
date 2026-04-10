update fw.dq_testcases
set benchmark_sql = 'select count(*) cnt from kdw.dwd_customer_cd_gp'
where object_name like 'kdw.customer_cd'
 and testcase_id = '233';

update fw.dq_testcases
set benchmark_sql = 'SELECT ID_CUSTOMER, TO_CHAR(DATE_CREATED ,''yyyy-mm-dd'') AS DATE_CREATED, TO_CHAR(DATE_MOD ,''yyyy-mm-dd'') AS DATE_MOD, TO_CHAR(PROC_DATE ,''yyyy-mm-dd'') AS PROC_DATE
FROM KDW.DWD_CUSTOMER_CD_GP
WHERE TRUNC(PROC_DATE) BETWEEN TRUNC(SYSDATE) - 8 AND TRUNC(SYSDATE) - 1
AND TRUNC(DATE_CREATED) < TRUNC(SYSDATE) AND TRUNC(DATE_MOD) < TRUNC(SYSDATE)'
where object_name like 'kdw.customer_cd'
 and testcase_id = '393';
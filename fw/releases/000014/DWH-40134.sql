UPDATE fw.dq_testcases
    set test_group='KDW_OTHER', active=FALSE
where dq_testcases.testcase_id=242
    and object_name='kdw.clt_type_refer';
UPDATE fw.dq_testcases
set test_group='KDW_REESTR', active=TRUE
where dq_testcases.testcase_id=234
  and object_name='kdw.division_d';
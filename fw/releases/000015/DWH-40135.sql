UPDATE fw.dq_testcases
SET benchmark_sql = 'select to_char(order_date,''mm.yyyy'') dt, count(*) cnt from kdw.dwd_e_whse_t_h_gp where order_date >= add_months(trunc(sysdate,''mm''),-3) and order_date <= trunc(sysdate) group by to_char(order_date,''mm.yyyy'')'
WHERE testcase_id = 200;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select to_char(order_date,''mm.yyyy'') dt, count(*) cnt from kdw.dwd_e_whse_t_l_gp where order_date >= add_months(trunc(sysdate,''mm''),-3) and order_date <= trunc(sysdate) group by to_char(order_date,''mm.yyyy'')'
WHERE testcase_id = 201;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_main_vend_whse_gp'
WHERE testcase_id = 202;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_spec_po_gp'
WHERE testcase_id = 203;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_u_oper_gp'
WHERE testcase_id = 204;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_item_def_whse_gp'
WHERE testcase_id = 205;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_bdt_item_k_pr_gp'
WHERE testcase_id = 206;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_vend_cont_gp'
WHERE testcase_id = 207;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_item_w_gp'
WHERE testcase_id = 208;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_price_list_gp'
WHERE testcase_id = 209;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.v_itn_stats_gp'
WHERE testcase_id = 210;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_zgl_stat_other_gp'
WHERE testcase_id = 211;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.v_itn_main_data_all_gp'
WHERE testcase_id = 212;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_trans_type_gp'
WHERE testcase_id = 213;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_trans_l_type_gp'
WHERE testcase_id = 214;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_vendor_gp'
WHERE testcase_id = 215;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_promoa_gp'
WHERE testcase_id = 216;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_item_promoa_gp'
WHERE testcase_id = 217;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_bdt_item_pr_gp'
WHERE testcase_id = 218;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_bdt_item_pr_gp'
WHERE testcase_id = 219;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_cause_item_move_gp'
WHERE testcase_id = 220;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_po_l_other_gp'
WHERE testcase_id = 221;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_spec_other_gp'
WHERE testcase_id = 222;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_firm_gp'
WHERE testcase_id = 224;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_bw_spec_po_gp'
WHERE testcase_id = 227;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_customer_cd_gp'
WHERE testcase_id = 233;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_division_gp'
WHERE testcase_id = 234;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_item_d_gp'
WHERE testcase_id = 235;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_curr_rate2_gp'
WHERE testcase_id = 236;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_price_list_gp'
WHERE testcase_id = 237;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_bdt_brand_gp'
WHERE testcase_id = 239;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_bdt_trade_mark_gp'
WHERE testcase_id = 240;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_business_region_gp'
WHERE testcase_id = 241;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_cust_contact_gp'
WHERE testcase_id = 243;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_div_form_gp'
WHERE testcase_id = 244;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_div_segment_gp'
WHERE testcase_id = 245;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_item_k_g_gp'
WHERE testcase_id = 246;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_item_pr_d_gp'
WHERE testcase_id = 247;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_territoria_gp'
WHERE testcase_id = 248;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_load_static_data_gp'
WHERE testcase_id = 249;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_m_assist_gp'
WHERE testcase_id = 250;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_m_assist_hist_gp'
WHERE testcase_id = 251;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_ord_other_gp'
WHERE testcase_id = 252;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_users_gp'
WHERE testcase_id = 253;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwe_item_g_gp'
WHERE testcase_id = 254;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_region_gp'
WHERE testcase_id = 255;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_term_cash_gp'
WHERE testcase_id = 256;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_trade_channel_gp'
WHERE testcase_id = 257;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_whse_gp'
WHERE testcase_id = 258;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_usd_rate_gp'
WHERE testcase_id = 260;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_region_gp'
WHERE testcase_id = 262;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_prm_set_gp'
WHERE testcase_id = 263;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_division_cur_gp'
WHERE testcase_id = 264;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_e_item_gp'
WHERE testcase_id = 265;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_goods_gp'
WHERE testcase_id = 266;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_calendar_gp'
WHERE testcase_id = 267;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwd_set_values_gp'
WHERE testcase_id = 268;

UPDATE fw.dq_testcases
SET benchmark_sql = 'select count(*) cnt from kdw.dwf_new_orderentries_e_gp'
WHERE testcase_id = 270;
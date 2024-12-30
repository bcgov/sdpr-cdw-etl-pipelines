"create table em_temp_bu1 as select a.*,  " : 
"            b.level1_descr as n_level1_descr,  " : 
"            b.level2_descr as n_level2_descr, " : 
"            b.level3_descr as n_level3_descr, " : 
"            b.level4_descr as n_level4_descr, " : 
"            b.level5_descr as n_level5_descr, " : 
"            b.level6_descr as n_level6_descr, " : 
"            b.level7_descr as n_level7_descr, " : 
"            b.bu_bk_descr as n_bu_bk_descr " : 
"            from ods.em_temp_empl_movement a left join cdw.or_business_unit_d b  " : 
"            on a.n_deptid=b.bu_deptid  " : 
"            and a.effdt between b.eff_date and nvl(b.end_date,sysdate);"
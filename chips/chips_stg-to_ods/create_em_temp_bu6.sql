"create table ods.em_temp_bu6 as select a.*,  " : 
"            b.level1_descr as n_level1_descr,  " : 
"            b.level2_descr as n_level2_descr, " : 
"            b.level3_descr as n_level3_descr, " : 
"            b.level4_descr as n_level4_descr, " : 
"            b.level5_descr as n_level5_descr, " : 
"            b.level6_descr as n_level6_descr, " : 
"            b.level7_descr as n_level7_descr, " : 
"            b.bu_bk_descr as n_bu_bk_descr " : 
"            from ods.em_temp_bu3 a left join ods.em_temp_bu5 b  " : 
"            on a.n_deptid=b.bu_deptid;"
create table ods.em_temp_bu8 as 
    select a.*,  
        b.level1_descr as p_level1_descr,  
        b.level2_descr as p_level2_descr, 
        b.level3_descr as p_level3_descr, 
        b.level4_descr as p_level4_descr, 
        b.level5_descr as p_level5_descr, 
        b.level6_descr as p_level6_descr, 
        b.level7_descr as p_level7_descr, 
        b.bu_bk_descr as p_bu_bk_descr 
    from ods.em_temp_bu7 a 
    left join cdw.or_business_unit_d b  
        on a.p_deptid=b.bu_deptid  
            and a.effdt between b.eff_date and nvl(b.end_date,sysdate)
;
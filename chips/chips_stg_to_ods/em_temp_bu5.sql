create table ods.em_temp_bu5 as 
    select * 
    from cdw.OR_BUSINESS_UNIT_D 
    where bu_deptid||to_char(eff_date,'YYYYMMDD') in (
        select bu_deptid||to_char(eff_date,'YYYYMMDD') 
        from ods.em_temp_bu4
    )
;
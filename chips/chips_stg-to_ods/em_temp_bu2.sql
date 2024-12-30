create table em_temp_bu2 as 
    select * 
    from ods.em_temp_bu1 
    where n_level1_descr is not null
;
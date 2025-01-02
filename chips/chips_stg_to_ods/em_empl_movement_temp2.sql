create table ods.em_empl_movement_temp2 compress as 
    select * from ods.em_temp_bu9 
    union 
    select * from ods.em_temp_bu11
;
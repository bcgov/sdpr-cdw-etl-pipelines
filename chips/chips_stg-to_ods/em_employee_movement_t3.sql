create table ods.em_employee_movement_t3 as 
    select a.*, b.descr as n_can_noc_descr, c.descr as p_can_noc_descr 
    from EM_EMPL_MOVEMENT_temp2 a 
    left join ods.em_can_noc_tbl b 
        on a.n_can_noc_cd=b.can_noc_cd 
    left join ods.em_can_noc_tbl c 
        on a.p_can_noc_cd=c.can_noc_cd
;
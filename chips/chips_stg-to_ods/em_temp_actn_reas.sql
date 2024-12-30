create table ods.em_temp_actn_reas as 
    select distinct action, actiondescr 
    from chips_stg.d_action_reason
;
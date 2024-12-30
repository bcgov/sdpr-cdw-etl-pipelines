create table ods.em_temp_act_reas1 as (
    select action, action_reason, max(effdt) as effdt 
    from CHIPS_STG.PS_ACTN_REASON_TBL 
    where eff_status='A' 
    group by action, action_reason
);
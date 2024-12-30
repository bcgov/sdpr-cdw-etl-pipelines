create table ods.em_temp_pos_dat1 as (
    select position_nbr, to_char(max(effdt),'YYYYMMDD') as effdt 
    from CHIPS_STG.PS_POSITION_DATA 
    where eff_status='A' 
    group by position_nbr
);
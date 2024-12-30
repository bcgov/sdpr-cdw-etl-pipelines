create table ods.em_emplid_2_idir as  
    with 
    
    y1 as (
        select emplid, emplid||to_char(max(lastupddttm),'YYYYMMDDHH24MISS') as emplid_key 
        from CHIPS_STG.PS_OPRDEFN_BC_TBL 
        group by emplid
    ) 

    select emplid, oprid 
    from CHIPS_STG.PS_OPRDEFN_BC_TBL 
    where emplid||to_char(lastupddttm,'YYYYMMDDHH24MISS') in (select emplid_key from y1)
;
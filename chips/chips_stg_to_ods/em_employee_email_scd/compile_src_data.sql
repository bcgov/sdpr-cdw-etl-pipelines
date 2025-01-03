with 

thedata as (
    select 
        emplid, 
        EMAILID,
    (
        select ls.pay_period_end_date 
        from cdw.chips_load_control lc
        join cdw.chips_load_sched ls 
            on lc.pay_period_end_date + 1 = ls.pay_period_start_date
        where lc.curr_load_ind =1
    ) pay_end_dt,
    (
        select ls.pay_period_start_date 
        from cdw.chips_load_control lc
        join cdw.chips_load_sched ls 
            on lc.pay_period_end_date + 1 = ls.pay_period_start_date
        where lc.curr_load_ind =1
    ) pay_start_dt,
    row_number() over (partition by emplid 
        order by 
            case when upper (OPRDEFNDESC) LIKE '%SDPR%' THEN 1 ELSE 2 END,
            case when upper (EMAILID) LIKE '%.GOV.BC.CA%' THEN 1 ELSE 2 END,
            VERSION DESC, 
            OPRID 
    ) RNK
        from CHIPS_STG.PS_OPRDEFN_BC_TBL WHERE trim(EMPLID) is not null and EMAILID > ' '
)

select emplid, emailid, pay_end_dt, pay_start_dt 
from thedata
where rnk = 1
order by EMPLID
;
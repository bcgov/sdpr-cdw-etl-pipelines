with 
thedata as (
    select emplid, "NAME", pay_end_dt, pay_end_dt - 13 PAY_START_DT,
        row_number() over (partition by emplid 
            order by off_cycle, update_dt desc, pay_sheet_src desc, paycheck_nbr desc
        ) rnk
    from chips_stg.ps_pay_check  
    WHERE PAY_END_DT = (
        SELECT PAY_PERIOD_END_DATE 
        FROM CDW.CHIPS_LOAD_CONTROL 
        WHERE CURR_LOAD_IND = 1
    )
)
select emplid, "NAME", pay_end_dt, PAY_START_DT
from thedata 
where rnk = 1
ORDER BY 1
;
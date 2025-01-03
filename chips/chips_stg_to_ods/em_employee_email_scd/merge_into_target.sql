Merge into ods.em_employee_email_scd target using (
    with 
    latest_load as (
        select ls.pay_period_start_date , ls.pay_period_end_date 
        from cdw.chips_load_control lc
        join cdw.chips_load_sched ls 
            on lc.pay_period_end_date + 1 = ls.pay_period_start_date
        where lc.curr_load_ind =1
    ),
    scd as (
        select emplid , min(start_pay_period) start_pay_period, max(end_pay_period) end_pay_period, count(*)
        from ods.em_employee_email_scd
        group by emplid 
        having count(*) =1 
    )
    select emplid , pay_period_start_date -14 prior_Start_date
    from scd 
    join latest_load ll 
        on ll.pay_period_start_date = scd.start_pay_period 
            and ll.pay_period_end_date = scd.end_pay_period
) src on (target.emplid = src.emplid)
when matched then update
set start_pay_period = src.prior_start_date
;
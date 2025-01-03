SELECT TO_CHAR(ls.pay_period_end_date, 'YYYYMMDD') 
from cdw.chips_load_control lc
join cdw.chips_load_sched ls 
    on lc.pay_period_end_date + 1 = ls.pay_period_start_date
where lc.curr_load_ind = 1
;
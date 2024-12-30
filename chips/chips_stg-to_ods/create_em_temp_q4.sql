"create table ods.em_temp_q4 as (select emplid,  " : 
"empl_rcd,  " : 
"action,  " : 
"action_reason,  " : 
"effdt,  " : 
"effseq,  " : 
"business_unit as n_business_unit,  " : 
"deptid as n_deptid,  " : 
"empl_status as n_status, " : 
"empl_ctg as n_appt_status, " : 
"jobcode as n_jobcode, " : 
"position_nbr as n_position_nbr, " : 
"tgb_base_position as n_base, " : 
"sal_admin_plan as n_sal_admin_plan, " : 
"grade as n_grade,  " : 
"step as n_step, " : 
"std_hours as n_std_hours, " : 
"hourly_rt as n_hourly_rt, " : 
"comprate as n_bi_weekly_rate, " : 
"annual_rt as n_annual_rt, " : 
"lag(effdt,1,to_date('19000101','YYYYMMDD')) over (partition by emplid order by effdt, effseq) as p_effdt, " : 
"lag(effseq, 1, null) over (partition by emplid order by effdt, effseq) as p_effseq, " : 
"lag(business_unit,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_business_unit, " : 
"lag(deptid,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_deptid, " : 
"lag(empl_status,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_status, " : 
"lag(empl_ctg,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_appt_status, " : 
"lag(jobcode,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_jobcode, " : 
"lag(position_nbr,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_position_nbr, " : 
"lag(tgb_base_position,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_base, " : 
"lag(sal_admin_plan,1,'New Hire') over (partition by emplid order by effdt, effseq) as p_sal_admin_plan, " : 
"lag(grade,1,'   ') over (partition by emplid order by effdt, effseq) as p_grade, " : 
"lag(step,1,null) over (partition by emplid order by effdt, effseq) as p_step, " : 
"lag(std_hours,1,0) over (partition by emplid order by effdt, effseq) as p_std_hours, " : 
"lag(hourly_rt,1,0.00) over (partition by emplid order by effdt, effseq) as p_hourly_rt, " : 
"lag(comprate,1,0.00) over (partition by emplid order by effdt, effseq) as p_bi_weekly_rate, " : 
"lag(annual_rt,1,0.00) over (partition by emplid order by effdt, effseq) as p_annual_rt " : 
"from ods.em_temp_q3);"
"create table ods.em_temp_empl_movement compress as select distinct " : 
"  a.emplid, " : 
"  c.empl_name, " : 
"  a.empl_rcd, " : 
"  a.effdt, " : 
"  a.effseq, " : 
"  a.action, " : 
"  nvl(n.actiondescr, case a.action when 'LTO' then 'Long Term Disability' when 'LTD' then 'Long Term Disability with Pay' when 'LOF' then 'Layoff' when 'STF' then 'Staffing' when 'RET' then 'Retirement' when 'PLA' then 'Paid Leave of Absence' else null end) as action_description, " : 
"  a.action_reason, " : 
"  d.descr as Reason_description, " : 
"  a.n_business_unit,  " : 
"  e.descr as n_Bus_Unit_Descr, " : 
"  a.n_deptid, " : 
"  f.descr as N_Dept_id_descr, " : 
"  a.n_status, " : 
"  o.descr as n_status_descr, " : 
"  o.status_grp as n_status_group, " : 
"  a.n_appt_status, " : 
"  p.appt_status_descr as n_appt_stat_descr, " : 
"  p.appointment_group as n_appt_grp_cd, " : 
"  p.appt_group_descr as n_appt_grp_descr, " : 
"  a.n_jobcode, " : 
"  h.descr as n_jobcode_descr, " : 
"  c.job_function as n_job_function, " : 
"  a.n_position_nbr, " : 
"  g.descr as n_position_title, " : 
"  g.can_noc_cd as n_can_noc_cd, " : 
"  a.n_base, " : 
"  a.n_sal_admin_plan, " : 
"  a.n_grade, " : 
"  a.n_step, " : 
"  a.n_std_hours, " : 
"  a.n_hourly_rt, " : 
"  a.n_bi_weekly_rate, " : 
"  a.n_annual_rt, " : 
"  a.p_effdt, " : 
"  a.p_effseq, " : 
"  a.p_business_unit, " : 
"  i.descr as p_Bus_Unit_Descr, " : 
"  a.p_deptid, " : 
"  j.descr as p_Dept_id_descr, " : 
"  a.p_status, " : 
"  q.descr as p_status_descr, " : 
"  q.status_grp as p_status_group, " : 
"  a.p_appt_status, " : 
"  r.appt_status_descr as p_appt_stat_descr, " : 
"  r.appointment_group as p_appt_grp_cd, " : 
"  r.appt_group_descr as p_appt_grp_descr, " : 
"  a.p_jobcode, " : 
"  k.descr as p_jobcode_descr, " : 
"  l.job_function as p_job_function, " : 
"  a.p_position_nbr, " : 
"  m.descr as p_position_title, " : 
"  m.can_noc_cd as p_can_noc_cd, " : 
"  a.p_base, " : 
"  a.p_sal_admin_plan, " : 
"  a.p_grade, " : 
"  a.p_step, " : 
"  a.p_std_hours, " : 
"  a.p_hourly_rt, " : 
"  a.p_bi_weekly_rate, " : 
"  a.p_annual_rt   " : 
"from ods.em_temp_q4 a left join chips_stg.ps_job_wrk b on a.emplid=b.emplid and a.n_position_nbr=b.position_nbr and a.effdt=b.effdt and a.effseq=b.effseq   " : 
"left join ods.em_temp_fte_burn2 c on a.emplid=c.emplid and a.n_business_unit=c.business_unit " : 
"left join ods.em_temp_act_reas2 d on a.action=d.action and a.action_reason=d.action_reason " : 
"left join CHIPS_STG.PS_BUS_UNIT_TBL_HR e on a.n_business_unit=e.business_unit " : 
"left join ods.em_temp_dept_tab2 f on a.n_deptid=f.deptid " : 
"left join ods.em_temp_pos_num1 g on a.n_position_nbr=g.position_nbr " : 
"left join ods.em_temp_jcode h on a.n_jobcode=h.jobcode and a.effdt between h.effdt and h.effenddt " : 
"left join CHIPS_STG.PS_BUS_UNIT_TBL_HR i on a.p_business_unit=i.business_unit " : 
"left join ods.em_temp_dept_tab2 j on a.p_deptid=j.deptid    " : 
"left join ods.em_temp_jcode k on a.p_jobcode=k.jobcode and a.effdt between k.effdt and k.effenddt " : 
"left join ods.em_temp_fte_burn2 l on a.emplid=l.emplid and a.p_business_unit=l.business_unit " : 
"left join ods.em_temp_pos_num1 m on a.p_position_nbr=m.position_nbr " : 
"left join ods.em_temp_actn_reas n on a.action=n.action " : 
"left join cdw.em_employee_status_d o on a.n_status=o.empl_status " : 
"left join cdw.em_appointment_status_d p on a.n_appt_status=p.appointment_status " : 
"left join cdw.em_employee_status_d q on a.p_status=q.empl_status " : 
"left join cdw.em_appointment_status_d r on a.p_appt_status=r.appointment_status " : 
"where (a.p_business_unit='BC031' or a.n_business_unit ='BC031') and a.effdt<=(select pay_end_dt from ods.em_temp_last_pay) order by emplid, effdt, effseq"
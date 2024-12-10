create table cperciva.HR_DATA_USAGE_AGREEMENT_SUBMISSION (
  employee varchar2(100 char),
  status varchar2(20 char),
  requires_fte_data varchar2(5 char),
  requires_stiip_data varchar2(5 char),
  requires_time_leave_data varchar2(5 char),
  requires_employee_data varchar2(5 char),
  requires_pay_cost_earnings_data varchar2(5 char),
  requires_employee_movement_data varchar2(5 char),
  requires_org_heirarchy_data varchar2(5 char),
  requires_other_data varchar2(5 char),
  details_of_other_required_data varchar2(1000 char),
  duration_of_access varchar2(20 char),
  access_end_date date,
  rational_for_access varchar2(1000 char),
  submission_date date,
  employee_idir varchar2(100 char),
  employee_position varchar2(100 char),
  employee_branch varchar2(100 char),
  employee_divison varchar2(100 char),
  employee_email varchar2(100 char),
  supervisor_name varchar2(100 char),
  supervisor_position varchar2(100 char),
  supervisor_email varchar2(100 char),
  deleted varchar2(5 char)
)
TABLESPACE "USERS"
;
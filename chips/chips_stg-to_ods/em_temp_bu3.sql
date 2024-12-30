create table em_temp_bu3 as 
    select 
        EMPLID, 
        EMPL_NAME, 
        EMPL_RCD, 
        EFFDT, 
        EFFSEQ, 
        ACTION, 
        ACTION_DESCRIPTION, 
        ACTION_REASON, 
        REASON_DESCRIPTION, 
        N_BUSINESS_UNIT, 
        N_BUS_UNIT_DESCR, 
        N_DEPTID, 
        N_DEPT_ID_DESCR, 
        N_STATUS, 
        N_STATUS_DESCR, 
        N_STATUS_GROUP, 
        N_APPT_STATUS, 
        N_APPT_STAT_DESCR, 
        N_APPT_GRP_CD, 
        N_APPT_GRP_DESCR, 
        N_JOBCODE, 
        N_JOBCODE_DESCR, 
        N_JOB_FUNCTION, 
        N_POSITION_NBR, 
        N_POSITION_TITLE, 
        N_CAN_NOC_CD, 
        N_BASE, 
        N_SAL_ADMIN_PLAN, 
        N_GRADE, 
        N_STEP, 
        N_STD_HOURS, 
        N_HOURLY_RT, 
        N_BI_WEEKLY_RATE, 
        N_ANNUAL_RT, 
        P_EFFDT, 
        P_EFFSEQ, 
        P_BUSINESS_UNIT, 
        P_BUS_UNIT_DESCR, 
        P_DEPTID, 
        P_DEPT_ID_DESCR, 
        P_STATUS, 
        P_STATUS_DESCR, 
        P_STATUS_GROUP, 
        P_APPT_STATUS, 
        P_APPT_STAT_DESCR, 
        P_APPT_GRP_CD, 
        P_APPT_GRP_DESCR, 
        P_JOBCODE, 
        P_JOBCODE_DESCR, 
        P_JOB_FUNCTION, 
        P_POSITION_NBR, 
        P_POSITION_TITLE, 
        P_CAN_NOC_CD, 
        P_BASE, 
        P_SAL_ADMIN_PLAN, 
        P_GRADE, 
        P_STEP, 
        P_STD_HOURS, 
        P_HOURLY_RT, 
        P_BI_WEEKLY_RATE, 
        P_ANNUAL_RT 
    from ods.em_temp_bu1 
    where n_level1_descr is null
;
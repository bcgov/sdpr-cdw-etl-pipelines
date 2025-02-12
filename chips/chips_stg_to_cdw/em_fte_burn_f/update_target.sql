UPDATE EM_FTE_BURN_F 
SET "FTE_REG" = ORCHESTRATE."FTE_REG", 
    "FTE_OVT" = ORCHESTRATE."FTE_OVT", 
    "FIRE_OVT" = ORCHESTRATE."FIRE_OVT", 
    "EMPL_SID" = ORCHESTRATE."EMPL_SID" 
WHERE "APPOINTMENT_STATUS_SID" = ORCHESTRATE."APPOINTMENT_STATUS_SID" 
    and "LOCATION_SID" = ORCHESTRATE."LOCATION_SID"
    and "PAY_END_DT_SK" = ORCHESTRATE."PAY_END_DT_SK"
    and "EMPL_STATUS_SID" = ORCHESTRATE."EMPL_STATUS_SID"
    and "POSITION_SID" = ORCHESTRATE."POSITION_SID"
    and "JOB_CLASS_SID" = ORCHESTRATE."JOB_CLASS_SID"
    and "BU_SID" = ORCHESTRATE."BU_SID"
    and "EMPLID" = ORCHESTRATE."EMPLID"
;
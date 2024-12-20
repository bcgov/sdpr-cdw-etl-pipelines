merge into IRSD_MYSS_INTAKE_FLOW TARGET using ( 
    select
      SR.ICM_SR_NUM, 
      'Applicant Override' OVERRIDE_CODE 
      from MCP_STG.TAAAOS_AAE_APP_OVRD_SERV_RQST ASR 
      join MCP_STG.TAASRQ_AAE_SERVICE_RQST SR on ASR.TAASRQ_ID = SR.TAASRQ_ID 
      join ods.irsd_myss_intake_flow i on i.IA_SR_NUM = SR.ICM_SR_NUM  
) SRC on (TARGET.IA_SR_NUM = SRC.ICM_SR_NUM ) 
when MATCHED then update 
    SET target.OVERRIDE_CODE = SRC.OVERRIDE_CODE
;

commit;
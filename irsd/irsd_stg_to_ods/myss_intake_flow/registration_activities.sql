MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET  
USING (  
SELECT /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/  
 i.REG_sr_wid,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'VALIDATE ID' THEN APAD.RESULT_CD           ELSE NULL END) REG_VALIDATE_RESULT_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'VALIDATE ID' THEN APAD.TODO_ACTL_END_DT    ELSE NULL END) REG_VALIDATE_DT,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'VERIFY EA FLAG' THEN APAD.RESULT_CD        ELSE NULL END) REG_VERIFY_EA_RESULT_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'VERIFY EA FLAG' THEN APAD.TODO_ACTL_END_DT ELSE NULL END) REG_VERIFY_EA_DT,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'SET PORTAL ACCOUNT FLAG, SEND WELCOME MESSAGE' THEN APAD.TODO_ACTL_END_DT ELSE NULL END) MYSS_PROFILE_LINKED_DT  
       from ods.irsd_myss_intake_flow i  
       INNER JOIN ICM_STG.WC_ACT_PLAN_ACTIVITY_F APAF  ON I.REG_ACT_PLAN_WID        = APAF.ACT_PLAN_WID  
       INNER JOIN ICM_STG.WC_ACT_PLAN_ACTIVITY_D APAD  ON APAF.ACT_PLAN_ACT_WID     = APAD.ROW_WID  
Where  
     APAF.DELETE_FLG  = 'N' AND  
     APAD.DELETE_FLG  = 'N' AND  
     Upper(APAD.ACTION_CD)IN ( 'VALIDATE ID', 'VERIFY EA FLAG','SET PORTAL ACCOUNT FLAG, SEND WELCOME MESSAGE')  
group by   
       i.REG_sr_wid  
) SRC ON (TARGET.REG_SR_WID = SRC.REG_SR_WID )  
WHEN MATCHED THEN UPDATE  
SET    
 target.REG_VALIDATE_RESULT_CD   = SRC.REG_VALIDATE_RESULT_CD,  
 target.REG_VALIDATE_DT    = SRC.REG_VALIDATE_DT,  
 target.REG_VERIFY_EA_RESULT_CD  = SRC.REG_VERIFY_EA_RESULT_CD,  
 target.REG_VERIFY_EA_DT   = SRC.REG_VERIFY_EA_DT,  
       target.MYSS_PROFILE_LINKED_DT     = SRC.MYSS_PROFILE_LINKED_DT
;

commit;

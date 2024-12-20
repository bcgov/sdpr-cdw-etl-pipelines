MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET  
USING (  
SELECT /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/  
 i.REG_sr_wid,  
  ROW_NUMBER() OVER ( PARTITION BY REG_sr_wid  
    ORDER BY   
      Case APD.STATUS_CD  
        When 'In Progress' then 1   
        when 'Open'        then 1   
        When 'Pending'     Then 1  
        When 'Closed'      Then 2  
        When 'Cancelled'   Then 3  
        Else                    4  
        End,  
      NVL(APD.COMPLETED_DT, APD.START_DT ) DESC,   
      APD.START_DT     DESC                            
  ) PLAN_RNK,  
       max(APF.ACT_PLAN_WID ) REG_ACT_PLAN_WID,  
       max(APF.CREATED_DT)    REG_ACT_PLAN_CREATE_DT  
       from ods.irsd_myss_intake_flow i  
       INNER JOIN ICM_STG.WC_ACTIVITY_PLAN_F APF       ON I.REG_SR_WID              = APF.SR_WID  
       INNER JOIN ICM_STG.WC_ACTIVITY_PLAN_D APD       ON APD.ROW_WID               = APF.ACT_PLAN_WID  
Where  
    APD.PLAN          = 'EA Portal Access' AND  
/*When porting this over from DM to DS the date below was set to the variable ICM_4_2_5_DATE - which was hard coded to the date below*/
    APF.CREATED_DT   >=  To_Date('2017-Feb-25 12:00:00 AM','YYYY-Mon-DD HH:MI:SS AM') 
group by   
       i.REG_sr_wid,  
       APD.STATUS_CD,  
       NVL(APD.COMPLETED_DT, APD.START_DT ),  
       APD.START_DT  
) SRC ON (TARGET.REG_SR_WID = SRC.REG_SR_WID AND SRC.PLAN_RNK = 1 )  
WHEN MATCHED THEN UPDATE  
SET     
  target.REG_ACT_PLAN_WID       = SRC.REG_ACT_PLAN_WID,  
      target.REG_ACT_PLAN_CREATE_DT = SRC.REG_ACT_PLAN_CREATE_DT

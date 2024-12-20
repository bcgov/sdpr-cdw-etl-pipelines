merge into IRSD_MYSS_INTAKE_FLOW TARGET using ( 
    select /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
        i.ia_sr_wid, 
        max(
            case 
                when Upper(APAD.ACTION_CD) in (
                        'WORKER VALIDATES CONTACT', 
                        'WORKER VALIDATES CONSENT'
                    ) 
                    and Upper(APAD.RESULT_CD) in (
                        'VALID CONTACT DOCUMENT PENDING',
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT'
                    ) 
                then 'TRUE' 
                else 'FALSE' 
            end
        )  ELECTRONIC_SIGNATURE_FLG 
    from ods.irsd_myss_intake_flow i 
    inner join ICM_STG.WC_ACT_PLAN_ACTIVITY_F APAF 
        on I.IA_ACT_PLAN_WID = APAF.ACT_PLAN_WID  
            and APAF.DELETE_FLG = 'N'  
    inner join ICM_STG.WC_ACT_PLAN_ACTIVITY_D APAD 
        on APAF.ACT_PLAN_ACT_WID = APAD.ROW_WID  
            and APAD.DELETE_FLG = 'N'  
            and Upper(APAD.ACTION_CD) IN ('WORKER VALIDATES CONTACT','DID THE WORKER VALIDATE THE CONTACT?', 'WORKER VALIDATES CONSENT', 'DID THE WORKER VALIDATE THE CONSENT?' ) 
            and APAD.TODO_ACTL_END_DT IS NOT NULL  
    group by  i.ia_sr_wid 
) SRC on (TARGET.IA_SR_WID = SRC.IA_SR_WID  ) 
when MATCHED then update 
SET target.ELECTRONIC_SIGNATURE_FLG = SRC.ELECTRONIC_SIGNATURE_FLG
;

commit;
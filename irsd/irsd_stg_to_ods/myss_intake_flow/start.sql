MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET USING ( 
    SELECT /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
        i.ia_sr_wid, 
        max(
            case 
                when Upper(APAD.ACTION_CD) IN (
                    'DID THE WORKER VALIDATE THE CONTACT?', 
                    'DID THE WORKER VALIDATE THE CONSENT?'
                ) 
                    and Upper(APAD.RESULT_CD) in (
                        'CIP PROCESS COMPLETE', 
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT', 
                        'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                    )
                    THEN APAD.TODO_ACTL_END_DT 
                when Upper(APAD.ACTION_CD) IN (
                    'WORKER VALIDATES CONTACT', 
                    'WORKER VALIDATES CONSENT'
                )  
                    and Upper(APAD.RESULT_CD) in (
                        'CIP PROCESS COMPLETE', 
                        'VALID CONTACT DOCUMENT PENDING',
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT', 
                        'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                    ) 
                    THEN APAD.TODO_ACTL_END_DT 
                ELSE NULL
            END
        ) keep (dense_rank first order by ( 
            case 
                when Upper(APAD.ACTION_CD) IN (
                        'WORKER VALIDATES CONTACT', 
                        'WORKER VALIDATES CONSENT'
                    ) 
                    and Upper(APAD.RESULT_CD) in (
                        'CIP PROCESS COMPLETE', 
                        'VALID CONTACT DOCUMENT PENDING',
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT', 
                        'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                    ) 
                    THEN 2 
                when Upper(APAD.ACTION_CD) IN (
                        'DID THE WORKER VALIDATE THE CONTACT?', 
                        'DID THE WORKER VALIDATE THE CONSENT?'
                    )   
                    and Upper(APAD.RESULT_CD) in (
                        'CIP PROCESS COMPLETE', 
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT', 
                        'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                    ) 
                    THEN 1 
                ELSE 3  
            END
            ) 
        ) START_COMPLETION_DT, 
        max(
            case 
                when Upper(APAD.ACTION_CD) IN (
                        'DID THE WORKER VALIDATE THE CONTACT?', 
                        'DID THE WORKER VALIDATE THE CONSENT?'
                    )  
                    and Upper(APAD.RESULT_CD) in (
                        'CIP PROCESS COMPLETE', 
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT', 
                        'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                    ) 
                    THEN APAD.OWNER_LOGIN  
                when Upper(APAD.ACTION_CD) IN (
                        'WORKER VALIDATES CONTACT', 
                        'WORKER VALIDATES CONSENT'
                    ) 
                    and Upper(APAD.RESULT_CD) in (
                        'CIP PROCESS COMPLETE', 
                        'VALID CONTACT DOCUMENT PENDING',
                        'VALID CONTACT WITH DOCUMENTS', 
                        'VALID ELECTRONIC CONSENT', 
                        'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                    ) THEN APAD.OWNER_LOGIN  
                ELSE NULL 
            END
        ) keep (dense_rank first order by ( 
                case 
                    when Upper(APAD.ACTION_CD) IN (
                            'WORKER VALIDATES CONTACT', 
                            'WORKER VALIDATES CONSENT'
                        ) 
                        and Upper(APAD.RESULT_CD) in (
                            'CIP PROCESS COMPLETE', 
                            'VALID CONTACT DOCUMENT PENDING',
                            'VALID CONTACT WITH DOCUMENTS', 
                            'VALID ELECTRONIC CONSENT', 
                            'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                        )  
                        THEN 2 
                    when Upper(APAD.ACTION_CD) IN (
                            'DID THE WORKER VALIDATE THE CONTACT?', 
                            'DID THE WORKER VALIDATE THE CONSENT?'
                        ) 
                        and Upper(APAD.RESULT_CD) in (
                            'CIP PROCESS COMPLETE', 
                            'VALID CONTACT WITH DOCUMENTS', 
                            'VALID ELECTRONIC CONSENT', 
                            'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT'
                        ) 
                        THEN 1 
                    ELSE 3  
                END
            ) 
        )                                                                                                               START_IDIR 

    from ods.irsd_myss_intake_flow i 
    INNER JOIN ICM_STG.WC_ACT_PLAN_ACTIVITY_F APAF 
        ON I.IA_ACT_PLAN_WID = APAF.ACT_PLAN_WID  
            and APAF.DELETE_FLG = 'N'  
    INNER JOIN ICM_STG.WC_ACT_PLAN_ACTIVITY_D APAD 
        ON APAF.ACT_PLAN_ACT_WID = APAD.ROW_WID  
            and APAD.DELETE_FLG = 'N'  
            AND Upper(APAD.ACTION_CD) IN ('WORKER VALIDATES CONTACT','DID THE WORKER VALIDATE THE CONTACT?', 'WORKER VALIDATES CONSENT', 'DID THE WORKER VALIDATE THE CONSENT?' ) 
            AND APAD.TODO_ACTL_END_DT IS NOT NULL  
    /* MUST have a EM record ( PLMS ) */
    JOIN ODS.IRSD_MYSS_PLMS_USERS O 
        ON OWNER_LOGIN = O.LEAF_IDIR 
            and TODO_ACTL_END_DT between PAY_END_DT and Next_pay_end_dt 
    group by  i.ia_sr_wid 
) SRC ON (TARGET.IA_SR_WID = SRC.IA_SR_WID) 
WHEN MATCHED THEN UPDATE 
SET target.START_COMPLETION_DT = SRC.START_COMPLETION_DT, 
    target.START_IDIR = SRC.START_IDIR
;

commit;
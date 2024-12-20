MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET USING (
    SELECT /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
        IA_SR_WID,  
        ROW_NUMBER() over (
            partition BY IA_SR_WID ORDER BY PMT_ISS_DT, PMT_ID1_NUM,PMT_ID2_NUM
            ) AS FIRST_ROW_NUM, 
        CHEQUE_ISSUE_TYPE_DESC, 
        PMT_ISS_DT, 
        PAYMENT_STATUS_DESC, 
        PAYMENT_DISTRIBUTION_GRP_DESCR  
    from ( 
    select 
        IA_SR_WID, 
        PMT_ID1_NUM, 
        PMT_ID2_NUM, 
        F.PMT_ISS_DT , 
        CI.CHEQUE_ISSUE_TYPE_DESC, 
        P.PAYMENT_STATUS_DESC, 
        PD.PAYMENT_DISTRIBUTION_GRP_DESCR 
    FROM ods.IRSD_MYSS_INTAKE_FLOW A  
    JOIN CDW.FN_PAYMENT_F F 
        ON F.FIL_CD = SUBSTR(A.ia_case_x_legacy_file_num, 1, 2) 
            AND F.FIL_NUM = SUBSTR(A.ia_case_x_legacy_file_num, 3, 8) 
            AND F.PMT_ISS_DT >= trunc(A.APPLICATION_SUBMISSION_DT) 
            and F.PMT_ISS_DT < trunc(a.NEXT_SR_CREATED_ON_DT) 
    JOIN CDW.FN_CHEQUE_ISSUE_TYPE_D CI 
        ON CI.CHEQUE_ISSUE_TYPE_SK = F.CHEQUE_ISSUE_TYPE_SK  
    JOIN CDW.FN_PAYMENT_STATUS_D P 
        ON P.PAYMENT_STATUS_SK = F.PAYMENT_STATUS_SK 
    JOIN CDW.FN_PAYMENT_DISTRIBUTION_D PD 
        ON PD.PAYMENT_DISTRIBUTION_SK = F.PAYMENT_DISTRIBUTION_SK 
    WHERE  a.ia_case_x_legacy_file_num is not null 
        and a.OPN_CASE_CONF_ELIG_RESULT_CD in ('Eligible', 'UA Eligible PWD') 
    union  
    select 
        myss.IA_SR_WID, 
        a.PMT_ID1_NUM, 
        a.PMT_ID2_NUM, 
        b.PMT_ISS_DT, 
        nvl(ci.cheque_issue_type_desc, 'UNK') cheque_issue_type_desc, 
        nvl(P.PAYMENT_STATUS_DESC, 'Unknown' ) PAYMENT_STATUS_DESC, 
        nvl(PD.PAYMENT_DISTRIBUTION_GRP_DESCR, 'Not Specified') PAYMENT_DISTRIBUTION_GRP_DESCR 
    FROM ods.IRSD_MYSS_INTAKE_FLOW MySS   
    join MIS_STG.TPHIST_FULL_TABLE a 
        ON a.FIL_ID_NUM = MySS.ia_case_x_legacy_file_num
            and a.PMT_ISS_CD in ('O' ,'E')                                      
    join  MIS_STG.TPAYMT_FULL_TABLE b 
        on a.PMT_ID1_NUM = b.PMT_ID1_NUM 
            and a.PMT_ID2_NUM = b.PMT_ID2_NUM 
            AND b.PMT_ISS_DT >= trunc(MySS.APPLICATION_SUBMISSION_DT)  
            and b.PMT_ISS_DT < trunc(MySS.NEXT_SR_CREATED_ON_DT) 
            and not (b.pmt_iss_num is null and a.pmt_iss_cd='O')                                      
    left join CDW.FN_CHEQUE_ISSUE_TYPE_D CI    
        ON CI.CHEQUE_ISSUE_TYPE_cd = a.pmt_iss_cd  
    left join CDW.FN_PAYMENT_STATUS_D P        
        ON P.PAYMENT_STATUS_cd = b.PMT_STAT_CD  
    left join CDW.FN_PAYMENT_DISTRIBUTION_D PD 
        ON PD.PAYMENT_DISTRIBUTION_CD = b.PMT_DISTBN_CD 
    WHERE MySS.ia_case_x_legacy_file_num is not null    
        and MySS.OPN_CASE_CONF_ELIG_RESULT_CD in ('Eligible', 'UA Eligible PWD') 
    )
) SRC ON (TARGET.IA_SR_WID = SRC.IA_SR_WID AND SRC.FIRST_ROW_NUM = 1)
WHEN MATCHED THEN UPDATE
SET TARGET.FIRST_CHQ_ISSUE_DT = SRC.PMT_ISS_DT,
    TARGET.FIRST_CHQ_ISSUE_STATUS_CD = SRC.PAYMENT_STATUS_DESC,    
    TARGET.FIRST_CHQ_ISSUE_TYPE = SRC.CHEQUE_ISSUE_TYPE_DESC ,
    TARGET.FIRST_CHQ_DISTRIBUTION = SRC.PAYMENT_DISTRIBUTION_GRP_DESCR
;

commit;
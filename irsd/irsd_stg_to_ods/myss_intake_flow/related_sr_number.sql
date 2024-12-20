MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET USING ( 
    SELECT  /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
        i.ia_SR_wid,rsr.SR_NUM,
        row_number() over (partition by ia_sr_num order by rsr.created_on_dt ) rnk 
    From ods.irsd_myss_intake_flow i  
    inner join icm_stg.WC_SR_REL_SR_XM xm 
        on xm.SR_WID = i.ia_SR_WID 
            and xm.delete_flg = 'N'  
    inner join icm_stg.w_srvreq_d rsr 
        on xm.REL_SR_WID = rsr.ROW_WID 
            and rsr.delete_flg = 'N' 
            and rsr.X_SR_CAT_TYPE_CD = 'Message' 
) SRC ON (TARGET.IA_SR_WID = SRC.IA_SR_WID AND SRC.RNK = 1) 
WHEN MATCHED THEN UPDATE 
SET target.RELATED_SR_NUM = SRC.SR_NUM
;

commit;
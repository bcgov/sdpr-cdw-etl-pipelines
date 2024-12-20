MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET USING ( 
    SELECT /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
    i.ia_sr_wid, 
    max('TRUE') FIRST_CONTACT_FLG, 
    max( F.ACTUAL_END_DT) KEEP (DENSE_RANK FIRST ORDER BY ACTUAL_END_DT) FIRST_CONTACT_DT, 
    max( E.LOGIN ) KEEP (DENSE_RANK FIRST ORDER BY ACTUAL_END_DT) FIRST_CONTACT_IDIR, 
    max( F.X_TYPE_CD ) KEEP (DENSE_RANK FIRST ORDER BY ACTUAL_END_DT) FIRST_CONTACT_SOURCE 
    from ods.irsd_myss_intake_flow i 
    join icm_stg.wc_sr_activity_xm x 
        on i.ia_sr_wid = x.sr_wid  
    join icm_stg.w_activity_f f 
        on x.activity_wid = f.row_wid 
            and f.owner_wid <> 0 
            and f.actual_end_dt is not null 
            and f.x_type_cd in ('Document', 'Document Request')  
            and f.ACTUAL_END_DT < NVL(i.FIRST_CONTACT_DT,TO_DATE('99991231','yyyymmdd')) 
    join icm_stg.w_employee_d e 
        on e.row_wid = f.owner_wid 
            and e.EMP_ACCNT_LOC <> 'INTERNAL' 
    group by i.ia_sr_wid 
) SRC ON (TARGET.IA_SR_WID = SRC.IA_SR_WID) 
WHEN MATCHED THEN UPDATE 
SET target.FIRST_CONTACT_FLG = SRC.FIRST_CONTACT_FLG, 
    target.FIRST_CONTACT_DT = SRC.FIRST_CONTACT_DT, 
    target.FIRST_CONTACT_IDIR = SRC.FIRST_CONTACT_IDIR, 
    target.FIRST_CONTACT_SOURCE = SRC.FIRST_CONTACT_SOURCE
;

commit;
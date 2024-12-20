merge into irsd_myss_intake_flow target using 
(
    select 
        pkey, 
        LEAD(IA_CREATED_ON_DT) over (
            partition by (ia_CASE_wid) order by IA_CREATED_ON_DT
            ) NEXT_SR_CREATED_ON_DT 
    from irsd_myss_intake_flow
) src ON (TARGET.pkey = SRC.pkey) 
WHEN MATCHED THEN UPDATE 
SET target.NEXT_SR_CREATED_ON_DT = NVL(SRC.NEXT_SR_CREATED_ON_DT,TO_DATE('99981231', 'YYYYMMDD'))
;

commit;
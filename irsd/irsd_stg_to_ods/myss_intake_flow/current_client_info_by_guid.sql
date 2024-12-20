MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET
USING ( 
    select /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/
        pKey,
        max(x.start_date) 
            keep (dense_rank first order by x.start_date desc nulls last) 
            LATEST_CASE_INVOLVEMENT_DT, 
        max(c.case_num) 
            keep (dense_rank first order by x.start_date desc nulls last) 
            last_CASE_NUM,
        nvl(max(
            case 
                when x.start_date < i.PORTAL_REQ_CRE_DTS 
                    and c.d_case_status_cd = 'Open' 
                    then 'TRUE' 
                else NULL 
            end
            ), 
            'FALSE'
        ) CURRENT_CLIENT_ID,
        max(
            case nvl(p.x_ea_prtacc_flg,'Z') 
                when 'N' then 'Not Linked in ICM' 
                when 'Y' then 'Linked in ICM' 
                else 'Not Found in ICM' 
            end
        ) keep (dense_rank first order by p.x_ea_prtacc_flg desc nulls last, x.start_date desc nulls last ) ICM_LINKED_STATUS
    from irsd_myss_intake_flow i 
    join icm_stg.w_party_per_d p 
        on i.client_bceid_guid = p.x_con_guid 
    LEFT join irsd_case_per_xm_scd x 
        on p.row_wid = x.contact_wid 
        and x.case_type = 'Employment and Assistance' 
        and x.relationship_cd in ('Key player' ,' Spouse')
        and x.start_date < least(nvl(i.PORTAL_REQ_CRE_DTS, to_date('99991231','yyyymmdd')),nvl( reg_created_on_dt,to_date('99991231','yyyymmdd')))
    LEFT join irsd_case c 
        on x.case_wid = c.case_wid
    where CLIENT_INFO_PROCESSED IS NULL
        and REG_SR_NUM is not null
    GROUP BY pKey
) SRC ON (TARGET.pKey = SRC.pKey)
WHEN MATCHED THEN UPDATE
SET TARGET.LATEST_CASE_INVOLVEMENT_DT = SRC.LATEST_CASE_INVOLVEMENT_DT,
    TARGET.CURRENT_CLIENT_ID = SRC.CURRENT_CLIENT_ID,
    TARGET.ICM_LINKED_STATUS = SRC.ICM_LINKED_STATUS,
    TARGET.CLIENT_INFO_PROCESSED = NVL(TARGET.CLIENT_INFO_PROCESSED, 0) + 2
where TARGET.CLIENT_INFO_PROCESSED IS NULL
;

commit;
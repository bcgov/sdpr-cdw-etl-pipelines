merge into IRSD_MYSS_INTAKE_FLOW TARGET using ( 
    select /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/
        pKey,
        nvl(
            max(
                case 
                    when x.start_date < i.PORTAL_REQ_CRE_DTS 
                        then 'TRUE' 
                    else NULL 
                    end
            ),
            'FALSE'
        ) EXISTING_CLIENT
    from irsd_myss_intake_flow i 
    LEFT join icm_stg.w_party_per_d p 
        on i.client_bceid_guid = p.x_con_guid 
    LEFT join irsd_case_per_xm_scd x 
        on  p.row_wid = x.contact_wid 
            and x.case_type = 'Employment and Assistance' 
            and x.relationship_cd in ('Key player' ,' Spouse')
    GROUP BY pKey
) SRC ON (TARGET.pKey = SRC.pKey)
WHEN MATCHED THEN UPDATE
SET 
    TARGET.EXISTING_CLIENT  = SRC.EXISTING_CLIENT
;

commit;
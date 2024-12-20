merge INTO IRSD_MYSS_INTAKE_FLOW TARGET USING ( 
    SELECT pkey,  
    /*Diversion Points*/ 
    Case 
        when IA_STATUS = 'Open' 
            then '1. Not Submitted'     
        when IA_CLOSE_DT is NULL 
            and IA_CREATED_ON_DT is not NULL 
            then '2. In-Progress' 
        when IA_CLOSE_DT is not  NULL 
            and OPN_CASE_CONF_ELIG_DT is not NULL 
            and OPN_CASE_CONF_ELIG_RESULT_CD is not NULL 
            then '8. Not Diverted' 
        when IA_CLOSE_DT is not NULL 
            and VALIDATION_DT is not NULL 
            and FIRST_CONTACT_DT is not NULL 
            and ((
                    OPN_CASE_CONF_ELIG_DT is not NULL 
                    and OPN_CASE_CONF_ELIG_RESULT_CD is NULL
                ) 
                or (
                    OPN_CASE_CONF_ELIG_DT is NULL 
                    and (
                        WORK_SEARCH_COMPL_DT is not NULL 
                        or Upper(WORK_SEARCH_REQUIRED_RESULT_CD) IN (
                            'WORK SEARCH EXEMPTION CRITERIA MET',
                            'WORK SEARCH COMPLETE',
                            'NOT REQUIRED', 
                            'WORK SEARCH COMPLETED PRIOR TO APPLICATION'
                        ) 
                    ) 
                )
            )
            then '7. Diversion before Eligibility Determination' 
        when IA_CLOSE_DT is not  NULL 
            and WORK_SEARCH_REQUIRED_DT is not NULL 
            and WORK_SEARCH_COMPL_DT is NULL 
            and FIRST_CONTACT_DT is not NULL  
            and VALIDATION_DT  is not NULL 
            and Upper(WORK_SEARCH_REQUIRED_RESULT_CD) not IN (
                'WORK SEARCH EXEMPTION CRITERIA MET',
                'WORK SEARCH COMPLETE',
                'NOT REQUIRED', 
                'WORK SEARCH COMPLETED PRIOR TO APPLICATION'
            ) 
            then '6. Diversion during Work Search' 
        when IA_CLOSE_DT is not NULL 
            and VALIDATION_DT is NULL 
            and FIRST_CONTACT_DT is not NULL
            then '5. Diversion before Validation' 
        when (
                IA_CLOSE_DT is not NULL 
                and FIRST_CONTACT_DT is NULL 
                and SUBM_DT is not NULL
            ) 
            or (
                IA_SR_WID is NULL 
                and SUBM_DT is not NULL
            ) 
            then '4. Diversion before First Contact' 
        when (
                IA_CLOSE_DT is not NULL 
                and SUBM_DT is NULL
            ) 
            or (
                IA_SR_WID is NULL 
                and SUBM_DT is NULL
            ) 
            then '3. Diversion before Submission'       
        /*all the rest if for closed Intake SRs*/ 
        /*default should not happen */
        Else '? Unknown ?' 
    End as DIVERSION_PHASE, 

    Case 
        when IA_STATUS = 'Open' 
            then NULL     
        when IA_CLOSE_DT is NULL 
            and IA_CREATED_ON_DT is not NULL
            then NULL 
        when IA_CLOSE_DT is not NULL 
            and OPN_CASE_CONF_ELIG_DT is not NULL 
            and OPN_CASE_CONF_ELIG_RESULT_CD is not NULL 
            then NULL 
        when IA_CLOSE_DT is not NULL 
            and VALIDATION_DT is not NULL 
            and FIRST_CONTACT_DT is not NULL 
            and ((
                    OPN_CASE_CONF_ELIG_DT is not NULL 
                    and OPN_CASE_CONF_ELIG_RESULT_CD is NULL
                ) 
                or (
                    OPN_CASE_CONF_ELIG_DT is NULL 
                    and (
                        WORK_SEARCH_COMPL_DT is not NULL 
                        or Upper(WORK_SEARCH_REQUIRED_RESULT_CD) IN (
                            'WORK SEARCH EXEMPTION CRITERIA MET',
                            'WORK SEARCH COMPLETE',
                            'NOT REQUIRED', 
                            'WORK SEARCH COMPLETED PRIOR TO APPLICATION'
                        ) 
                    ) 
                ) 
            ) 
            then IA_CLOSE_DT 
        when IA_CLOSE_DT is not NULL 
            and WORK_SEARCH_REQUIRED_DT is not NULL 
            and WORK_SEARCH_COMPL_DT is NULL 
            and FIRST_CONTACT_DT is not NULL   
            and VALIDATION_DT is NOT NULL 
            and Upper(WORK_SEARCH_REQUIRED_RESULT_CD) NOT IN (
                'WORK SEARCH EXEMPTION CRITERIA MET',
                'WORK SEARCH COMPLETE',
                'NOT REQUIRED', 
                'WORK SEARCH COMPLETED PRIOR TO APPLICATION'
            ) 
            then IA_CLOSE_DT 
        when IA_CLOSE_DT is not NULL 
            and VALIDATION_DT is NULL 
            and FIRST_CONTACT_DT is not NULL 
            then IA_CLOSE_DT 
        when IA_CLOSE_DT is not  NULL 
            and FIRST_CONTACT_DT is NULL
            then IA_CLOSE_DT 
        when IA_SR_WID is NULL      
            and SUBM_DT is not NULL  
            then SUBM_DT 
        when IA_SR_WID is NULL 
            and SUBM_DT is NULL
            then UPDT_DTS 
        when IA_CLOSE_DT is not NULL 
            and SUBM_DT is NULL 
            then IA_CLOSE_DT       
        /*all the rest if for closed Intake SRs*/ 
        /*default should not happen */
        Else NULL 
    End as DIVERSION_DT, 

    Case when IA_STATUS = 'Open' then NULL    /* NOT SUBMITTED */
        when IA_CLOSE_DT is NULL 
            and IA_CREATED_ON_DT is not NULL 
            then NULL /* IN PROGRESS */ 
        when IA_CLOSE_DT is not NULL 
            and OPN_CASE_CONF_ELIG_DT is not NULL 
            and OPN_CASE_CONF_ELIG_RESULT_CD is not NULL 
            then NULL /* NOT DIVERTED*/ 
        when IA_CLOSE_DT is not NULL 
            and OPN_CASE_CONF_ELIG_DT is not NULL 
            and OPN_CASE_CONF_ELIG_RESULT_CD is NULL 
            and VALIDATION_DT  is NOT NULL 
            then OPN_CASE_CONF_ELIG_IDIR  
        when IA_CLOSE_DT is not NULL 
            and OPN_CASE_CONF_ELIG_DT is NULL 
            and VALIDATION_DT is not NULL 
            and FIRST_CONTACT_DT is not NULL 
            and (
                WORK_SEARCH_COMPL_DT is not NULL 
                or Upper(WORK_SEARCH_REQUIRED_RESULT_CD) IN (
                    'WORK SEARCH EXEMPTION CRITERIA MET',
                    'WORK SEARCH COMPLETE',
                    'NOT REQUIRED',
                    'WORK SEARCH COMPLETED PRIOR TO APPLICATION'
                ) 
            )  
            then NVL(WORK_SEARCH_COMPL_IDIR,
                NVL(WORK_SEARCH_REQUIRED_IDIR, 
                    NVL(VALIDATION_IDIR,
                        NVL(FIRST_CONTACT_IDIR, 
                            NVL(PROSPECTING_IDIR, 
                                NVL(IA_X_OWNER,'Unknown')
                            )
                        )
                    )
                )
            ) /*then '7. Diversion before Eligibility Determination'*/ 
        when IA_CLOSE_DT is not NULL 
            and WORK_SEARCH_REQUIRED_DT is not NULL 
            and WORK_SEARCH_COMPL_DT is NULL 
            and FIRST_CONTACT_DT is not NULL   
            and VALIDATION_DT  is not NULL 
            and WORK_SEARCH_REQUIRED_RESULT_CD not IN (
                'WORK SEARCH EXEMPTION CRITERIA MET',
                'WORK SEARCH COMPLETE',
                'NOT REQUIRED', 
                'WORK SEARCH COMPLETED PRIOR TO APPLICATION'
            ) 
            then NVL(WORK_SEARCH_REQUIRED_IDIR, 
                NVL(VALIDATION_IDIR,
                    NVL(FIRST_CONTACT_IDIR, 
                        NVL(PROSPECTING_IDIR, 
                            NVL(IA_X_OWNER,'Unknown')
                        )
                    )
                )
            ) /* '6. Diversion during Work Search' */
        when IA_CLOSE_DT is not NULL 
            and VALIDATION_DT is NULL 
            and FIRST_CONTACT_DT is not NULL 
            then FIRST_CONTACT_IDIR /* then '5. Diversion before Validation'*/ 
        when IA_CLOSE_DT is not NULL 
            and FIRST_CONTACT_DT is NULL
            and SUBM_DT is not NULL  
            then NVL(PROSPECTING_IDIR,
                NVL(IA_X_OWNER, 'SIEBEL EAI')
            ) /* '4. Diversion before First Contact'*/ 
        when IA_SR_WID is NULL 
            and SUBM_DT is not NULL 
            then 'SIEBEL EAI'  /* '4. Diversion before First Contact'*/ 
        when IA_CLOSE_DT is not NULL 
            and SUBM_DT is NULL 
            then IA_X_OWNER       /*'3. Diversion before Submission'*/
        when IA_SR_WID is NULL 
            and SUBM_DT is NULL 
            then 'SIEBEL EAI'       /*'3. Diversion before Submission'*/ 
        /*all the rest if for closed Intake SRs */
        /*default should not happen*/ 
        else NULL 
    end as DIVERSION_IDIR 
    
    from ODS.IRSD_MYSS_INTAKE_FLOW  
    where IA_SR_NUM is not NULL 
) SRC on (SRC.PKEY = TARGET.PKEY) 
when MATCHED then update 
SET TARGET.DIVERSION_PHASE = SRC.DIVERSION_PHASE, 
    TARGET.DIVERSION_DT    = SRC.DIVERSION_DT, 
    TARGET.DIVERSION_IDIR  = SRC.DIVERSION_IDIR
;

commit;
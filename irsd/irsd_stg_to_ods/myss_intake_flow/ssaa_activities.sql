merge INTO IRSD_MYSS_INTAKE_FLOW TARGET  
USING (  
SELECT /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/  
 i.ia_sr_wid,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORKER OVERIDE' and Upper(APAD.RESULT_CD) = 'YES' THEN 'Worker Override' ELSE I.OVERRIDE_CODE  END)   OVERRIDE_CODE,  
   max(case when Upper(APAD.ACTION_CD) = 'PROSPECT PROMOTION' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'          then APAD.RESULT_CD  
           when Upper(APAD.ACTION_CD) = 'PROMOTE PROSPECT TO CONTACT' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'  then APAD.RESULT_CD  
           ELSE NULL END)    
        keep (dense_rank first order by ( case when Upper(APAD.ACTION_CD) = 'PROSPECT PROMOTION' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'   then 1  
           when Upper(APAD.ACTION_CD) = 'PROMOTE PROSPECT TO CONTACT' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'                              then 2  
           ELSE 3 END) )                                                                                                                         PROSPECTING_RESULT_CD,  
  max(case when Upper(APAD.ACTION_CD) = 'PROSPECT PROMOTION' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'           then APAD.TODO_ACTL_END_DT  
           when Upper(APAD.ACTION_CD) = 'PROMOTE PROSPECT TO CONTACT' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'  then APAD.TODO_ACTL_END_DT  
           ELSE NULL END)    
       keep (dense_rank first order by ( case when Upper(APAD.ACTION_CD) = 'PROSPECT PROMOTION' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'   then 1  
           when Upper(APAD.ACTION_CD) = 'PROMOTE PROSPECT TO CONTACT' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'                              then 2  
           ELSE 3 END) )                                                                                                                         PROSPECTING_DT,  
  max(case when Upper(APAD.ACTION_CD) = 'PROSPECT PROMOTION' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'           then APAD.OWNER_LOGIN   
           when Upper(APAD.ACTION_CD) = 'PROMOTE PROSPECT TO CONTACT' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'  then APAD.OWNER_LOGIN    
           ELSE NULL END)    
         keep (dense_rank first order by ( case when Upper(APAD.ACTION_CD) = 'PROSPECT PROMOTION' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'  then 1  
           when Upper(APAD.ACTION_CD) = 'PROMOTE PROSPECT TO CONTACT' and Upper(APAD.RESULT_CD) = 'CONTACT CREATED'                              then 2  
           ELSE 3 END) )                                                                                                                         PROSPECTING_IDIR,  
  max(case when Upper(APAD.ACTION_CD) in ( 'DID THE WORKER VALIDATE THE CONTACT?' , 'DID THE WORKER VALIDATE THE CONSENT?')  
                 and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')  THEN APAD.TODO_ACTL_END_DT  
           when Upper(APAD.ACTION_CD) in ('WORKER VALIDATES CONTACT' , 'WORKER VALIDATES CONSENT')  
                 and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')  THEN APAD.TODO_ACTL_END_DT  
           ELSE NULL  END)      
          keep (dense_rank first order by (case when Upper(APAD.ACTION_CD) in ('WORKER VALIDATES CONTACT' , 'WORKER VALIDATES CONSENT')   
                 and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')                 THEN 2  
           when Upper(APAD.ACTION_CD) in ( 'DID THE WORKER VALIDATE THE CONTACT?' , 'DID THE WORKER VALIDATE THE CONSENT?')  
                 and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')                 THEN 1  
           ELSE 3 END))                                                                                                                                                                                                      VALIDATION_DT,  
  max(case when Upper(APAD.ACTION_CD) in ( 'DID THE WORKER VALIDATE THE CONTACT?'  , 'DID THE WORKER VALIDATE THE CONSENT?')  
                 and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')  THEN APAD.OWNER_LOGIN   
           when Upper(APAD.ACTION_CD) in ('WORKER VALIDATES CONTACT' , 'WORKER VALIDATES CONSENT')  
                 and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')  THEN APAD.OWNER_LOGIN   
           ELSE NULL END)  
          keep (dense_rank first order by (case when Upper(APAD.ACTION_CD) in ('WORKER VALIDATES CONTACT' , 'WORKER VALIDATES CONSENT')  
                                            and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')        THEN 2  
           when Upper(APAD.ACTION_CD) in ( 'DID THE WORKER VALIDATE THE CONTACT?' , 'DID THE WORKER VALIDATE THE CONSENT?')  
                   and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS', 'VALID ELECTRONIC CONSENT', 'VALID WRITTEN HR0080B OR HR0080A VERBAL CONSENT')                                 THEN 1  
           ELSE 3 END))                                                                                                                                                                                                    VALIDATION_IDIR,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'DETERMINE IF START PROCESS CAN PROCEED' AND Upper(APAD.RESULT_CD) = 'COMPLETE' then 'TRUE' ELSE 'FALSE' END)    AUTO_START_FLG,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'DETERMINE IF START PROCESS CAN PROCEED' THEN APAD.RESULT_CD   ELSE NULL END)                                    START_RESULT_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'DETERMINE IF START PROCESS CAN PROCEED' THEN APAD.TODO_ACTL_END_DT ELSE NULL  END)                              START_BEGIN_DT,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?'   THEN APAD.RESULT_CD   ELSE NULL END)                                                   WORK_SEARCH_REQUIRED_RESULT_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?'   THEN APAD.TODO_PLAN_START_DT ELSE NULL  END)                                           WORK_SEARCH_REQUIRED_DT,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?'   THEN APAD.OWNER_LOGIN ELSE NULL  END)                                                  WORK_SEARCH_REQUIRED_IDIR,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' THEN APAD.RESULT_CD   ELSE NULL END)                                                     WORK_SEARCH_COMPL_RESULT_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and APAD.RESULT_CD is not NULL  THEN APAD.TODO_ACTL_END_DT ELSE NULL  END)               WORK_SEARCH_COMPL_DT,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and APAD.RESULT_CD is not NULL  THEN APAD.OWNER_LOGIN ELSE NULL  END)                    WORK_SEARCH_COMPL_IDIR,  
  max(CASE When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) in ('NOT REQUIRED', 'WORK SEARCH EXEMPTION CRITERIA MET') and IA_X_PUBLISH_CD = 'INA' Then 'INA Work Search Exempt'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) in ('NOT REQUIRED', 'WORK SEARCH EXEMPTION CRITERIA MET')                             Then 'Work Search Exempt'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and Upper(APAD.RESULT_CD) = 'NOT REQUIRED' and IA_X_PUBLISH_CD = 'INA'                                          Then 'INA Work Search Exempt'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and Upper(APAD.RESULT_CD) = 'NOT REQUIRED'                                                                      Then 'Work Search Exempt'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and (IA_X_PUBLISH_CD = 'INA'  or Upper(APAD.RESULT_CD) ='INA')                                                  Then 'INA Work Search'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) like '%WEEK WORK SEARCH REQUIRED' and IA_X_PUBLISH_CD = 'INA'                         Then 'INA Work Search'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) like '%WEEK WORK SEARCH REQUIRED'                                                     Then 'Work Search'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) = 'WORK SEARCH COMPLETE' and IA_X_PUBLISH_CD = 'INA'                                  Then 'INA Work Search'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) = 'WORK SEARCH COMPLETE'                                                              Then 'Work Search'  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?'                                                                                                                 Then 'Work Search'  
           When (Upper(APAD.ACTION_CD) = 'WORKER VALIDATES CONTACT'   
                  and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS' ))  
                OR   
                (Upper(APAD.ACTION_CD) = 'DID THE WORKER VALIDATE THE CONTACT?'   
                  and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS') )         Then 'Unspecified'  
         ELSE 'Not Validated' END )     
          keep (dense_rank first order by (  CASE When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) in ('NOT REQUIRED', 'WORK SEARCH EXEMPTION CRITERIA MET') and IA_X_PUBLISH_CD = 'INA' Then 1  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) in ('NOT REQUIRED', 'WORK SEARCH EXEMPTION CRITERIA MET')                             Then 2  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and Upper(APAD.RESULT_CD) = 'NOT REQUIRED' and IA_X_PUBLISH_CD = 'INA'                                          Then 3  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and Upper(APAD.RESULT_CD) = 'NOT REQUIRED'                                                                      Then 4  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?' and (IA_X_PUBLISH_CD = 'INA'  or Upper(APAD.RESULT_CD) ='INA')                                                  Then 5  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) like '%WEEK WORK SEARCH REQUIRED' and IA_X_PUBLISH_CD = 'INA'                         Then 6  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) like '%WEEK WORK SEARCH REQUIRED'                                                     Then 7  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) = 'WORK SEARCH COMPLETE' and IA_X_PUBLISH_CD = 'INA'                                  Then 8  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH REQUIRED?' and Upper(APAD.RESULT_CD) = 'WORK SEARCH COMPLETE'                                                              Then 9  
           When Upper(APAD.ACTION_CD) = 'WORK SEARCH COMPLETE?'                                                                                                          Then 10  
           When (Upper(APAD.ACTION_CD) = 'WORKER VALIDATES CONTACT'   
             and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS' ))  
                OR   
                (Upper(APAD.ACTION_CD) = 'DID THE WORKER VALIDATE THE CONTACT?'   
             and Upper(APAD.RESULT_CD) in ('CIP PROCESS COMPLETE','VALID CONTACT WITH DOCUMENTS') )         Then 11  
           else  12 end) )                                                                                                                       WORK_SEARCH_GRP_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'OPEN CASE AND CONFIRM ELIGIBILITY'  THEN APAD.RESULT_CD   ELSE NULL END)                                                       OPEN_CASE_CONF_ELIG_RESULT_CD,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'OPEN CASE AND CONFIRM ELIGIBILITY'  and APAD.RESULT_CD is not NULL THEN APAD.TODO_ACTL_END_DT ELSE NULL  END)                  OPEN_CASE_CONF_ELIG_DT,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'OPEN CASE AND CONFIRM ELIGIBILITY'  and APAD.RESULT_CD is not NULL THEN APAD.OWNER_LOGIN ELSE NULL END)                        OPEN_CASE_CONF_ELIG_IDIR,  
  max(CASE WHEN Upper(APAD.ACTION_CD) = 'SET THE EA FLAG TO Y AND SEND WELCOME MESSAGE'   
            AND Upper(APAD.RESULT_CD) = 'COMPLETE' THEN APAD.TODO_ACTL_END_DT ELSE NULL  END)                                                           EA_FLG_SEND_WELCOME_MSG_DT,  
     max(case when Upper(APAD.ACTION_CD) = 'UPDATE START SET LOV TO PENDING CONSENT' then 'TRUE' else 'FALSE' end)                                      START_LOV_RETURNED_FLG  
FROM ODS.IRSD_MYSS_INTAKE_FLOW I  
INNER JOIN ICM_STG.WC_ACT_PLAN_ACTIVITY_F APAF  ON I.IA_ACT_PLAN_WID         = APAF.ACT_PLAN_WID  
INNER JOIN ICM_STG.WC_ACT_PLAN_ACTIVITY_D APAD  ON APAF.ACT_PLAN_ACT_WID     = APAD.ROW_WID  
Where   
      APAF.DELETE_FLG = 'N' AND  
       APAD.DELETE_FLG  = 'N' AND  
       Upper(APAD.ACTION_CD) IN ('WORKER OVERIDE' ,   
                          'PROSPECT PROMOTION' , 'PROMOTE PROSPECT TO CONTACT',   
                          'WORKER VALIDATES CONTACT','DID THE WORKER VALIDATE THE CONTACT?', 'WORKER VALIDATES CONSENT', 'DID THE WORKER VALIDATE THE CONSENT?' ,  
                          'DETERMINE IF START PROCESS CAN PROCEED',    
                          'WORK SEARCH REQUIRED?','WORK SEARCH COMPLETE?' ,  
                          'OPEN CASE AND CONFIRM ELIGIBILITY',   'OPEN CASE AND CONFIRM ELIGIBILITY',  
                          'SET THE EA FLAG TO Y AND SEND WELCOME MESSAGE'   ,  
                          'UPDATE START SET LOV TO PENDING CONSENT'  
                         )   
group by   
      i.ia_sr_wid  
) SRC ON (TARGET.IA_SR_WID = SRC.IA_SR_WID )  
WHEN MATCHED THEN UPDATE  
SET  target.OVERRIDE_CODE      = SRC.OVERRIDE_CODE,  
 target.PROSPECTING_RESULT_CD    = SRC.PROSPECTING_RESULT_CD,  
 target.PROSPECTING_DT       = SRC.PROSPECTING_DT,  
 target.PROSPECTING_IDIR       = SRC.PROSPECTING_IDIR,  
 target.VALIDATION_DT      = SRC.VALIDATION_DT,  
target.VALIDATION_IDIR      = SRC.VALIDATION_IDIR,  
 target.AUTO_START_FLG       = SRC.AUTO_START_FLG,  
 target.START_RESULT_CD      = SRC.START_RESULT_CD,  
 target.START_BEGIN_DT     = SRC.START_BEGIN_DT,  
/* Aug 12th Move START IDIR/Completion DT to Electronic Signature Node (PLMS User query) based on  UAT Enhancement  
 target.START_COMPLETION_DT      = SRC.START_COMPLETION_DT,  
 target.START_IDIR         = SRC.START_IDIR,  
*/  
 target.WORK_SEARCH_REQUIRED_RESULT_CD   = SRC.WORK_SEARCH_REQUIRED_RESULT_CD,  
 target.WORK_SEARCH_REQUIRED_DT    = SRC.WORK_SEARCH_REQUIRED_DT,  
 target.WORK_SEARCH_COMPL_RESULT_CD  = SRC.WORK_SEARCH_COMPL_RESULT_CD,  
 target.WORK_SEARCH_COMPL_DT     = SRC.WORK_SEARCH_COMPL_DT,  
 target.WORK_SEARCH_GRP      = SRC.WORK_SEARCH_GRP_CD,  
 target.OPN_CASE_CONF_ELIG_RESULT_CD   = SRC.OPEN_CASE_CONF_ELIG_RESULT_CD,  
 target.OPN_CASE_CONF_ELIG_DT    = SRC.OPEN_CASE_CONF_ELIG_DT,  
 target.OPN_CASE_CONF_ELIG_IDIR    = SRC.OPEN_CASE_CONF_ELIG_IDIR,  
 target.EA_FLG_SEND_WELCOME_MSG_DT     = SRC.EA_FLG_SEND_WELCOME_MSG_DT,  
       target.START_LOV_RETURNED_FLG            = SRC.START_LOV_RETURNED_FLG,  
 target.WORK_SEARCH_REQUIRED_IDIR    = SRC.WORK_SEARCH_REQUIRED_IDIR,  
 target.WORK_SEARCH_COMPL_IDIR          = SRC.WORK_SEARCH_COMPL_IDIR
;

commit;
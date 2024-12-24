insert into  ODS.IRSD_BUS_PASS_CUBE 
(
  select /*+ parallel */    
              INVOLVEMENT_ROLE
              , CONTACT_KEY
              ,  BEN_MTH_DT
              , MATCH_TYPE
              , TRANSIT_AREA_GRP
              , TRANSIT_AREA
              , nvl2(EA_CASE_WID, 'Unspecified',HAS_SPOUSE_FLG) HAS_SPOUSE_FLG
              , DA_CLIENT_TYPE_CURR
              , BP_CASE_WID, KP_BP_CASE_WID, SP_BP_CASE_WID, EA_CASE_WID, MIS_FILE_NUM
              , (nvl(sp_b_prev_count,0)) +  (nvl(kp_b_prev_count,0))                          bus_pass_prev_cnt
              , (nvl(sp_b_curr_count,0)) +  (nvl(kp_b_curr_count,0))                          bus_pass_curr_cnt
              , (nvl(DA_CLIENT_PREV_PASS_COUNT,0))	                                      "DA_CLIENT_PREV_PASS_COUNT"
              , (nvl(DA_CLIENT_CURR_PASS_COUNT,0))	                                      "DA_CLIENT_CURR_PASS_COUNT"
              , (nvl(DA_CLIENT_PREV_CASH_COUNT,0))	                                      "DA_CLIENT_PREV_CASH_COUNT"
              , (nvl(DA_CLIENT_CURR_CASH_COUNT,0))	                                      "DA_CLIENT_CURR_CASH_COUNT"
              , (nvl(DA_CLIENT_CURR_CASH_AMT,0))                                       "DA_CLIENT_CURR_CASH_AMT"
              ,  0 "KP_DA_PASS_DIFF"
              ,  0 "SP_DA_PASS_DIFF"
              ,  0 "KP_DA_CASH_DIFF"
              ,  0 "SP_DA_CASH_DIFF"
              , NVL(bp_approved_product_cd,'Unspecified')      bp_approved_product_cd
              , NVL(BP_WORK_QUEUE,'Unspecified')               BP_WORK_QUEUE
              , NVL(EA_WORK_QUEUE,'Unspecified')               EA_WORK_QUEUE
              , NVL(INAC_FLG,'Unspecified')                    INAC_FLG
 ,case 
  when EA_CASE_WID IS NOT NULL AND BP_CASE_WID IS NOT NULL AND MIS_FILE_NUM IS NOT NULL    THEN 'EA Case with Bus Pass and MIS'
  when EA_CASE_WID IS NOT NULL AND BP_CASE_WID IS  NULL AND MIS_FILE_NUM IS  NULL          THEN 'EA Case Only'
  when BP_CASE_WID IS NOT NULL AND MIS_FILE_NUM IS NULL AND EA_CASE_WID IS  NULL           THEN 'Bus Pass ONLY Case'
  when BP_CASE_WID IS NULL AND MIS_FILE_NUM IS Not NULL AND EA_CASE_WID IS  NULL          THEN 'MIS CASE ONLY Case' 
  when MIS_FILE_NUM IS NOT NULL   AND EA_CASE_WID IS NULL and BP_CASE_WID IS NOT NULL      THEN 'MIS File without matching EA Case'
  when EA_CASE_WID IS NOT NULL AND BP_CASE_WID IS NOT NULL AND MIS_FILE_NUM IS NULL        THEN 'EA Case with Bus Pass'
  when EA_CASE_WID IS NOT NULL AND MIS_FILE_NUM IS NOT NULL   and BP_CASE_WID IS  NULL     THEN 'EA Case with MIS'
  when BP_CASE_WID IS NOT NULL AND MIS_FILE_NUM IS NOT NULL  AND EA_CASE_WID IS NULL  THEN 'Bus Pass Case without matching EA Case'
  when BP_CASE_WID IS NOT NULL AND EA_CASE_WID IS NOT NULL  AND MIS_FILE_NUM IS  NULL     THEN 'Bus Pass Case without matching MIS Case'
   else 'Other' 
end                                             CLIENT_MATCHING
, case 
     when ben_mth_dt = to_date('20171201','yyyymmdd') then 'TSA' 
     else 'TS'
  end Policy_Period              
              , 0 "PASS_CANCELLED"
              , 0 "PASS_STARTED"
              , 0 "CASH_CANCELLED"
              , 0 "CASH_STARTED"
              , 0 "PASS_TO_CASH"
              , 0 "CASH_TO_PASS"
              , 0 "NET_PASS_CHANGE"
              , 0 "NET_CASH_CHANGE"
              , (nvl(KP_DA_PREV_PASS_COUNT,0))                                    "KP_DA_PREV_PASS_COUNT"
              , (nvl(KP_DA_PREV_CASH_COUNT,0))                                    "KP_DA_PREV_CASH_COUNT"
              , (nvl(KP_B_PREV_COUNT,0))                                          "KP_B_PREV_COUNT"
              , (nvl(SP_DA_PREV_PASS_COUNT,0))                                    "SP_DA_PREV_PASS_COUNT"
              , (nvl(SP_DA_PREV_CASH_COUNT,0))                                    "SP_DA_PREV_CASH_COUNT"
              , (nvl(SP_B_PREV_COUNT,0))                                          "SP_B_PREV_COUNT"
              , (nvl(KP_DA_CURR_PASS_COUNT,0))                                    "KP_DA_CURR_PASS_COUNT"
              , (nvl(KP_DA_CURR_CASH_COUNT,0))                                    "KP_DA_CURR_CASH_COUNT"
              , (nvl(KP_B_CURR_COUNT,0))                                          "KP_B_CURR_COUNT"
              , (nvl(SP_DA_CURR_PASS_COUNT,0))                                    "SP_DA_CURR_PASS_COUNT"
              , (nvl(SP_DA_CURR_CASH_COUNT,0))                                    "SP_DA_CURR_CASH_COUNT"
              , (nvl(SP_B_CURR_COUNT,0))                                          "SP_B_CURR_COUNT"
          , Cast('Unspecified' as varchar2(4000)) chq_exception
          , Cast('Unspecified' as varchar2(4000)) chq_src_exception
          , Cast('Unspecified' as varchar2(4000)) cp_exception
          , Cast('Unspecified' as varchar2(4000)) cp_src_exception
  
  from 
  (
      select      'KP' INVOLVEMENT_ROLE
                  , NVL(CAST(KP_CONTACT_WID AS VARCHAR2(30)), MIS_FILE_NUM) CONTACT_KEY
                  , BEN_MTH_DT
                  , MATCH_TYPE
                  , KP_BP_CASE_WID BP_CASE_WID,  KP_BP_CASE_WID, SP_BP_CASE_WID, EA_CASE_WID,MIS_FILE_NUM        
                  ,nvl(bp.KP_BP_TRANSIT_AREA_GRP,'Unspecified')                  "TRANSIT_AREA_GRP"
                  , NVL(BP.KP_BP_TRANSIT_AREA,'Unspecified')                     TRANSIT_AREA
                  , HAS_SPOUSE_FLG
                  , NVL(DA_CLIENT_TYPE_CURR,'Seniors or Others')                 DA_CLIENT_TYPE_CURR
                  ,NVL(kp_bp_approved_product_cd,'Unspecified')   bp_approved_product_cd
                  ,NVL(KP_BP_WORK_QUEUE,'Unspecified')            BP_WORK_QUEUE
                  , NVL(EA_WORK_QUEUE,'Unspecified')               EA_WORK_QUEUE
                  ,NVL(KP_INAC_FLG,'Unspecified')                 INAC_FLG
                  , 0                                                        "DA_CLIENT_PREV_PASS_COUNT"
                  , 0                                                        "DA_CLIENT_PREV_CASH_COUNT"
                  , ( bp.KP_MIS_PASS_CNT )                                   "DA_CLIENT_CURR_PASS_COUNT"
                  , ( bp.KP_MIS_CASH_CNT )                                   "DA_CLIENT_CURR_CASH_COUNT"
                  , 0                                                        "B_PREV_COUNT"
                  , ( bp.KP_BP_CNT )                                         "B_CURR_COUNT"
                  --
                  , null                                                        "KP_DA_PREV_PASS_COUNT"
                  , null                                                        "KP_DA_PREV_CASH_COUNT"
                  , null                                                        "KP_B_PREV_COUNT"
                  , null                                                        "SP_DA_PREV_PASS_COUNT"
                  , null                                                        "SP_DA_PREV_CASH_COUNT"
                  , null                                                        "SP_B_PREV_COUNT"
                  , ( bp.KP_MIS_PASS_CNT )                                   "KP_DA_CURR_PASS_COUNT"
                  , ( bp.KP_MIS_CASH_CNT )                                   "KP_DA_CURR_CASH_COUNT"
                  , ( bp.KP_BP_CNT )                                         "KP_B_CURR_COUNT"
                  , null                                                        "SP_DA_CURR_PASS_COUNT"
                  , null                                                        "SP_DA_CURR_CASH_COUNT"
                  , null                                                        "SP_B_CURR_COUNT"
                  , (DA_CLIENT_CURR_CASH_AMT)                                "DA_CLIENT_CURR_CASH_AMT"
      from        IRSD_BUS_PASS_ARCHIVE bp
      where       nvl(bp.KP_BP_CNT,0) + nvl(bp.KP_MIS_PASS_CNT,0) + nvl(bp.KP_MIS_CASH_CNT,0) > 0

      union all
      select      'SP' INVOLVMENT_ROLE
                  , CAST(SP_CONTACT_WID AS VARCHAR2(30)) CONTACT_KEY
                  , BEN_MTH_DT
                  , MATCH_TYPE
                  , SP_BP_CASE_WID BP_CASE_WID,  KP_BP_CASE_WID, SP_BP_CASE_WID, EA_CASE_WID,MIS_FILE_NUM           
                  ,nvl(bp.SP_BP_TRANSIT_AREA_GRP,'Unspecified')                  "TRANSIT_AREA_GRP"
                  , NVL(BP.SP_BP_TRANSIT_AREA,'Unspecified')                      TRANSIT_AREA
                  , HAS_SPOUSE_FLG
                  , NVL(DA_CLIENT_TYPE_CURR,'Seniors or Others')                 DA_CLIENT_TYPE_CURR
                  ,NVL(Sp_bp_approved_product_cd,'Unspecified')   bp_approved_product_cd
                  ,NVL(SP_BP_WORK_QUEUE,'Unspecified')            BP_WORK_QUEUE
                 , NVL(EA_WORK_QUEUE,'Unspecified')               EA_WORK_QUEUE
                 ,NVL(SP_INAC_FLG,'Unspecified')                 INAC_FLG
                  , 0                                                        "DA_CLIENT_PREV_PASS_COUNT"
                  , 0                                                        "DA_CLIENT_PREV_CASH_COUNT"
                  , ( bp.SP_MIS_PASS_CNT )                                   "DA_CLIENT_CURR_PASS_COUNT"
                  , ( bp.SP_MIS_CASH_CNT )                                   "DA_CLIENT_CURR_CASH_COUNT"
                  , 0                                                        "B_PREV_COUNT"
                  , ( bp.SP_BP_CNT )                                         "B_CURR_COUNT"
                  --
                  , null                                                        "KP_DA_PREV_PASS_COUNT"
                  , null                                                        "KP_DA_PREV_CASH_COUNT"
                  , null                                                        "KP_B_PREV_COUNT"
                  , null                                                        "SP_DA_PREV_PASS_COUNT"
                  , null                                                        "SP_DA_PREV_CASH_COUNT"
                  , null                                                        "SP_B_PREV_COUNT"
                  , null                                                        "KP_DA_CURR_PASS_COUNT"
                  , null                                                        "KP_DA_CURR_CASH_COUNT"
                  , null                                                        "KP_B_CURR_COUNT"
                  , ( bp.SP_MIS_PASS_CNT )                                   "SP_DA_CURR_PASS_COUNT"
                  , ( bp.SP_MIS_CASH_CNT )                                   "SP_DA_CURR_CASH_COUNT"
                  , ( bp.SP_BP_CNT )                                         "SP_B_CURR_COUNT"
                  , NULL                	                                      "DA_CLIENT_CURR_CASH_AMT"
      from        IRSD_BUS_PASS_ARCHIVE bp
      where       nvl(bp.SP_BP_CNT,0) + nvl(bp.SP_MIS_PASS_CNT,0) + nvl(bp.SP_MIS_CASH_CNT,0) > 0
/* found duplicate records for clients in a benefit month, they were key players on their own case at the time they were a spouse on another case */
      and not (  CAST(SP_CONTACT_WID AS VARCHAR2(30)) , BEN_MTH_DT )
                 in ( select     
                          NVL(CAST(KP_CONTACT_WID AS VARCHAR2(30)), MIS_FILE_NUM) CONTACT_KEY
                        , BEN_MTH_DT
                      from        IRSD_BUS_PASS_ARCHIVE bp1
                      where       nvl(bp1.KP_BP_CNT,0) + nvl(bp1.KP_MIS_PASS_CNT,0) + nvl(bp1.KP_MIS_CASH_CNT,0) > 0
                ) 
   )
);

commit;
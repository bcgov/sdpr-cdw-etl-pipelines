-- build temporary table of any Bus Pass Case Orders that were valid since "cohort" of 2016-08 (policy change date)
--  > valid Orders have 20-digit Serial Number and Start-End range over current month or last month
create table IRSD_BUS_PASS_O_CP_TMP nologging compress parallel as
  select /*+ parallel */ distinct 
    con.row_wid "CONTACT_WID"
    ,con.X_CONTACT_NUM
    ,con.DECEASE_FLG
    ,round(months_between(trunc(sysdate),con.BIRTH_DT)/12,2) "PERSON_AGE"
    ,case when con.BIRTH_DT < to_date(to_char(sysdate,'yyyy')-66||'1231','yyyymmdd') then 'Year after 65' end "PERSON_65_LAST_YEAR_CD"
    ,CON.X_PER_STATUS
    ,con.X_PWD_STAT_CD
    ,con.X_PWD_ADJUD_DT
    ,case 
    when substr(X_PER_STATUS,1,1)='H' then 'PWD' 
    --and round(months_between(sysdate,con.BIRTH_DT)/12,2) < 66 
    when substr(X_PER_STATUS,1,1) in ('B','O','R') 
    --and round(months_between(sysdate,con.BIRTH_DT)/12,2) < 66
    and X_PWD_STAT_CD='Eligible' then 'PWD'
    else 'not PDW' end "PERSON_PWD_CD"
    ,X_MSO_RSN_CD
    ,X_MSO_REVIEW_DT
    ,X_NXT_MON_BP_FLG
    ,X_ACTIVE_BP_FLG
    ,con.X_ABOR_BAND_OU_ID
    ,case 
    when X_ABOR_BAND_OU_ID !='Unspecified' 
    and X_ABOR_BAND_OU_ID != '999 - OUT OF PROVINCE' 
    and to_char(max(c.x_aandc_elig_dt) 
    over (partition by con.row_wid),'yyyy-mm') 
    >= to_char(sysdate,'yyyy')-1 || '-11'
    then 'INAC' 
    else 'not INAC' end "INAC_FLG"
    ,max(c.row_wid) over (partition by con.row_wid) "BP_CASE_WID"
    ,max(c.case_num) over (partition by con.row_wid) "BP_CASE_NUM"
    ,max(c.x_work_queue) over (partition by con.row_wid) "BP_WORK_QUEUE"
    ,max(c.x_aandc_elig_dt) over (partition by con.row_wid) "BP_AANDC_ELIG_DT"
    ,max(c.X_BCT_STICKER_FLG) over (partition by con.row_wid) "X_BCT_STICKER_FLG"
    ,case upper(nvl(max(X_TRANSIT_AREA) 
    over (partition by con.row_wid),'Unknown'))
    when 'UNSPECIFIED' then 'Unknown' 
    when 'UNKNOWN' then 'Unknown' 
    when 'VANCOUVER' then 'Translink' 
    else 'BC Transit' end "TRANSIT_AREA_GRP"
    ,max(X_TRANSIT_AREA) over (partition by con.row_wid) "TRANSIT_AREA"
    --
    ,max(o_d.x_serial_num) over (partition by con.row_wid) "MAX_SERIAL_NUM"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Cancelled')
    then o_d.x_serial_num end) over (partition by con.row_wid) "CANCELLED_SERIAL_NUM"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Cancelled')
    then oi.WRITE_IN_PRD_NAME end) over (partition by con.row_wid) "CANCELLED_PRODUCT_CD"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Cancelled')
    then o_d.W_UPDATE_DT end) over (partition by con.row_wid) "CANCELLED_DT"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Ready for Approval')
    then o_d.W_UPDATE_DT end) over (partition by con.row_wid) "READY_DT"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Pending')
    --              and length(o_d.x_serial_num) = 20
    --              and ois.INTEGRATION_STATE = 'Synced'
    then o_d.W_UPDATE_DT end) over (partition by con.row_wid) "PENDING_DT"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Approved')
    and length(o_d.x_serial_num) = 20
    and ois.INTEGRATION_STATE = 'Synced'
    then o_d.x_serial_num end) over (partition by con.row_wid) "APPROVED_SERIAL_NUM"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Approved')
    and length(o_d.x_serial_num) = 20
    and ois.INTEGRATION_STATE = 'Synced'
    then oi.WRITE_IN_PRD_NAME end) over (partition by con.row_wid) "APPROVED_PRODUCT_CD"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Approved')
    and length(o_d.x_serial_num) = 20
    and ois.INTEGRATION_STATE = 'Synced'
    then oi.X_SVC_PER_PROD_AMT end) over (partition by con.row_wid) "APPROVED_PRODUCT_AMT"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Approved')
    and length(o_d.x_serial_num) = 20
    and ois.INTEGRATION_STATE = 'Synced'
    then o_d.W_UPDATE_DT end) over (partition by con.row_wid) "APPROVED_DT"
    ,max(case 
    when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
      then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd')) -- (1a) Current
    and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Approved')--,'Pending')
    and length(o_d.x_serial_num) = 20
    and ois.INTEGRATION_STATE = 'Synced'
    then 'B' 
    when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
      then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    > last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd')) -- (2a) Future dated
    --                and o_d.X_EFF_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
    and o_d.STATUS_CD in ('Approved')--,'Pending')
    and length(o_d.x_serial_num) = 20
    and ois.INTEGRATION_STATE = 'Synced'
    and oi.WRITE_IN_PRD_NAME = 'Replacement' -- (2b) Replacement ONLY
    then 'B' 
    else '.' end) over (partition by con.row_wid) "VALID_FLG"
    ,first_value(case when to_char( o_d.X_EFF_START_DT,'yyyymm') 
    = to_char(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'yyyymm')
    then oi.WRITE_IN_PRD_NAME end) over (partition by con.row_wid order by o_d.ROW_WID) "PRODUCT_CD"
    ,max(case when  (case when o_d.X_APPROVAL_DT > o_d.X_EFF_START_DT 
    then o_d.X_APPROVAL_DT else o_d.X_EFF_START_DT end) 
    <= last_day(add_months(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'), 1))
    and o_d.X_EFF_END_DT >= trunc(add_months(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'), 1),'mm')
    and o_d.STATUS_CD in ('Approved','Pending')
    --              and length(o_d.x_serial_num) = 20
    --              and ois.INTEGRATION_STATE = 'Synced'
    then oi.WRITE_IN_PRD_NAME end) over (partition by con.row_wid) "NM_APPROVED_PRODUCT_CD"
    --
  from        ICM_STG.W_CASE_D c
  inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
  inner join  ICM_STG.WC_CASE_PER_XM con_xm on c.row_wid=con_xm.case_wid and con_xm.DELETE_FLG='N' 
  and     con_xm.END_DATE is null and con_xm.SUBJECT_FLG='Y' and con_xm.RELATIONSHIP_CD='Key player'
  inner join  ICM_STG.W_PARTY_PER_D con on con.ROW_WID=con_xm.CONTACT_WID and con.DELETE_FLG='N' 
  --
  inner join  ICM_STG.WC_BNFT_PLAN_CASE_XM bp_xm on bp_xm.case_wid=c.ROW_WID and bp_xm.DELETE_FLG='N' 
  inner join  ICM_STG.W_BNFT_PLAN_D bp on bp_xm.BNFT_PLAN_WID = bp.ROW_WID and bp.DELETE_FLG='N' 
  and     bp.STATUS_NAME in ('Approved')--,'Archived','Closed')  
  and     bp.BNFTPLAN_END_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
  and     bp.BNFTPLAN_START_DT <= last_day(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'))
  --
  inner join  ICM_STG.W_ORDER_F o_f on c.ROW_WID=o_f.case_wid 
  and     o_f.X_SEC_BU_ID = 'ICMDW-1000' and o_f.DELETE_FLG='N'
  --
  inner join  ICM_STG.W_ORDER_D o_d on o_d.ROW_WID=o_f.ORDER_WID and o_d.DELETE_FLG='N'
  and     o_d.X_EFF_END_DT  >= trunc(add_months(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'), 1),'mm')
  and     o_d.X_EFF_START_DT  <= last_day(add_months(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'), 1))
  and     o_d.STATUS_CD in ('Ready for Approval','Pending','Approved','Cancelled')--,'Archived','Closed')
  --
  inner join   ICM_STG.W_ORDERITEM_F oi on oi.order_wid=o_f.order_wid and oi.DELETE_FLG='N'
  and       oi.WRITE_IN_PRD_NAME = 'Client Portion'
  inner join   ICM_STG.WC_ORDER_INT_STATE_DM ois on ois.ORDER_WID= o_f.ORDER_WID 
  --
  where       c.DELETE_FLG = 'N'
  and         c.TYPE_CD='Bus Pass'
  ;



-- build temporary table of MIS Payments to Code 24 or 87 (joined above via Legacy File #)
create table IRSD_BUS_PASS_MIS_CP_TMP nologging compress  parallel as
select  /*+ parallel append */
--
        case when nvl(CD24_PREV_BEN_MTH_CNT,0) + nvl(CD87_PREV_BEN_MTH_CNT,0)
          + nvl(CD49_PREV_BEN_MTH_CNT,0) + nvl(CD49_PASS_PREV_BEN_MTH_CNT,0) > 0 then 'DA Case Receiving'
          || case when CD49_PASS_PREV_BEN_MTH_CNT  > 0   then ' TS (pass)'   end
          || case when CD49_PREV_BEN_MTH_CNT       > 0   then 
              case when CD49_PASS_PREV_BEN_MTH_CNT > 0   then ' and TS (cash)' else ' TS (cash)'   end end
          || case when CD24_PREV_BEN_MTH_CNT       > 0   
              and CD87_PREV_BEN_MTH_CNT < 1 then 
                case when CD49_PREV_BEN_MTH_CNT
                 + CD49_PASS_PREV_BEN_MTH_CNT      > 0   then ' and TSA (24)' else ' TSA (24)'  end end
          || case when CD87_PREV_BEN_MTH_CNT       > 0   then 
              case when CD49_PASS_PREV_BEN_MTH_CNT
                ||CD49_PREV_BEN_MTH_CNT > 0              then ' and' end
              ||case when CD24_PREV_BEN_MTH_CNT < 1 
                then ' TSA-In Kind (87) but without TSA (24)'  
                else ' TSA-In Kind (87)' end 
              end
          end                                                                   "DA_CLIENT_TYPE_PREV"
        ,case when nvl(CD24_CURR_BEN_MTH_CNT,0) + nvl(CD87_CURR_BEN_MTH_CNT,0)
          + nvl(CD49_CURR_BEN_MTH_CNT,0) + nvl(CD49_PASS_CURR_BEN_MTH_CNT,0) > 0 then 'DA Case Receiving'
          || case when CD49_PASS_CURR_BEN_MTH_CNT  > 0   then ' TS (pass)' else ' ' end
          || case when CD49_CURR_BEN_MTH_CNT       > 0   then 
              case when CD49_PASS_CURR_BEN_MTH_CNT > 0   then ' and TS (cash)' else ' TS (cash)'   end end
          || case when CD24_CURR_BEN_MTH_CNT       > 0   
              and CD87_CURR_BEN_MTH_CNT < 1 then 
                case when CD49_CURR_BEN_MTH_CNT
                 + CD49_PASS_CURR_BEN_MTH_CNT      > 0   then ' and TSA (24)' else ' TSA (24)'  end end
          || case when CD87_CURR_BEN_MTH_CNT       > 0   then 
              case when CD49_PASS_CURR_BEN_MTH_CNT
                ||CD49_CURR_BEN_MTH_CNT > 0              then ' and' end
              ||case when CD24_CURR_BEN_MTH_CNT < 1 
                then ' TSA-In Kind (87) but without TSA (24)'  
                else ' TSA-In Kind (87)' end 
              end
          end                                                                   "DA_CLIENT_TYPE_CURR"
        --
        ,CD49_PREV_BEN_MTH_CNT                                                  "DA_CLIENT_PREV_CASH_CNT"
        ,CD49_PASS_PREV_BEN_MTH_CNT                                             "DA_CLIENT_PREV_PASS_CNT"
        ,CD49_PREV_BEN_MTH_AMT                                                  "DA_CLIENT_PREV_CASH_AMT"
        --
        ,CD49_CURR_BEN_MTH_CNT                                                  "DA_CLIENT_CURR_CASH_CNT"
        ,CD49_PASS_CURR_BEN_MTH_CNT                                             "DA_CLIENT_CURR_PASS_CNT"
        ,CD49_CURR_BEN_MTH_AMT                                                  "DA_CLIENT_CURR_CASH_AMT"
        ,t.*
from
(
  select  MIS_FILE_NUM
--
          ,max(CD24_PREV_BEN_MTH_AMT) "CD24_PREV_BEN_MTH_AMT",      max(CD87_PREV_BEN_MTH_AMT) "CD87_PREV_BEN_MTH_AMT", max(CD49_PREV_BEN_MTH_AMT) "CD49_PREV_BEN_MTH_AMT"
          ,max(CD24_CURR_BEN_MTH_AMT) "CD24_CURR_BEN_MTH_AMT",      max(CD87_CURR_BEN_MTH_AMT) "CD87_CURR_BEN_MTH_AMT", max(CD49_CURR_BEN_MTH_AMT) "CD49_CURR_BEN_MTH_AMT"
--
          ,max(CD24_PREV_BEN_MTH_CNT) "CD24_PREV_BEN_MTH_CNT",      max(CD87_PREV_BEN_MTH_CNT) "CD87_PREV_BEN_MTH_CNT", max(DA_PREV_BEN_MTH_CD)  "DA_PREV_BEN_MTH_CD", max(CD49_PREV_BEN_MTH_CNT) "CD49_PREV_BEN_MTH_CNT", max(CD49_PASS_PREV_BEN_MTH_CNT) "CD49_PASS_PREV_BEN_MTH_CNT"
          ,max(CD24_CURR_BEN_MTH_CNT) "CD24_CURR_BEN_MTH_CNT",      max(CD87_CURR_BEN_MTH_CNT) "CD87_CURR_BEN_MTH_CNT", max(DA_CURR_BEN_MTH_CD)  "DA_CURR_BEN_MTH_CD", max(CD49_CURR_BEN_MTH_CNT) "CD49_CURR_BEN_MTH_CNT", max(CD49_PASS_CURR_BEN_MTH_CNT) "CD49_PASS_CURR_BEN_MTH_CNT"
  from
  (
  --
  --  Client opted for Code 49 Payment instead of Bus Pass 
  --
    select    /*+ parallel append */
              F.FIL_CD||F.FIL_NUM                                                 "MIS_FILE_NUM"
    --
              ,nvl(max(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and allowance_cd = '24' then round(item_adj_amt/52,0) end),0)    "CD24_PREV_BEN_MTH_CNT"
              ,nvl(sum(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and allowance_cd = '24' then item_adj_amt end),0)                "CD24_PREV_BEN_MTH_AMT"
              ,nvl(max(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and allowance_cd = '87' then round(item_adj_amt/-52,0) end),0)   "CD87_PREV_BEN_MTH_CNT"
              ,nvl(sum(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and allowance_cd = '87' then item_adj_amt end),0)                "CD87_PREV_BEN_MTH_AMT"
              ,max(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and f.CORE_BUS_SK=1 then 'DA' end)                                   "DA_PREV_BEN_MTH_CD"
              ,nvl(max(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and allowance_cd = '49' then round(item_adj_amt/52,0) end),0)    "CD49_PREV_BEN_MTH_CNT"
              ,nvl(sum(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and allowance_cd = '49' then item_adj_amt end),0)                "CD49_PREV_BEN_MTH_AMT"
              ,0                                                                                                                                                                                                        "CD49_PASS_PREV_BEN_MTH_CNT"
    
              ,nvl(max(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and allowance_cd = '24' then round(item_adj_amt/52,0) end),0)    "CD24_CURR_BEN_MTH_CNT"
              ,nvl(sum(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and allowance_cd = '24' then item_adj_amt end),0)                "CD24_CURR_BEN_MTH_AMT"
              ,nvl(max(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and allowance_cd = '87' then round(item_adj_amt/-52,0) end),0)   "CD87_CURR_BEN_MTH_CNT"
              ,nvl(sum(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and allowance_cd = '87' then item_adj_amt end),0)                "CD87_CURR_BEN_MTH_AMT"
              ,max(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and f.CORE_BUS_SK=1 then 'DA' end)                                   "DA_CURR_BEN_MTH_CD"
              ,nvl(max(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and allowance_cd = '49' then round(item_adj_amt/52,0) end),0)    "CD49_CURR_BEN_MTH_CNT"
              ,nvl(sum(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and allowance_cd = '49' then item_adj_amt end),0)                "CD49_CURR_BEN_MTH_AMT"
              ,0                                                                                                                                                                   "CD49_PASS_CURR_BEN_MTH_CNT"
--      
    from        CDW.FN_PAYMENT_ITEM_F      f
    inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
    where       f.ASST_MTH_PART_NUM >= to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm')
      and       f.ALLOWANCE_CD in ('87','24','49')--,'63') 
      and       f.POSTED_TO_GL_IND = 'Y'
    group by    F.FIL_CD||F.FIL_NUM
    having      sum(case when allowance_cd = '24' then item_adj_amt end) != 0
        or      sum(case when allowance_cd = '87' then item_adj_amt end) != 0
        or      sum(case when allowance_cd = '49' then item_adj_amt end) != 0
        or      max(case when f.ASST_MTH_PART_NUM=substr(m.ASOF_DATE_NAME,1,4) || substr(m.ASOF_DATE_NAME,6,2) and f.CORE_BUS_SK=1 then 'DA' end) = 'DA'
        or      max(case when f.ASST_MTH_PART_NUM=to_char(add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1),'yyyymm') and f.CORE_BUS_SK=1 then 'DA' end) = 'DA'
    --
    union all
    --
    --  Client opted for Bus Pass instead of Code 49 Payment (PREV)
    --
    select      /*+ parallel append */ 
                t.FIL_ID_NUM                                                    "MIS_FILE_NUM"
                ,null, null, null, null, case when CAS_BUSPASS_CNT > 0 then 'BP' end, null, null
                ,CAS_BUSPASS_CNT                                                "CD49_PASS_PREV_BEN_MTH_CNT"
                ,null, null, null, null, null, null, null
                ,0                                                              "CD49_PASS_CURR_BEN_MTH_CNT"
    from        mis_stg.TPHIST_FULL_TABLE  t
    inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
    where       PMT_BNFT_MTH_DT = add_months(trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm'), -1)
      and       CAS_BUSPASS_CNT > 0
  --    and       PMT_BNFT_MTH_DT >= to_date(20180101,'yyyymmdd')                   -- "TS" started on 2018-Jan Benefit Month
      and       PMT_ISS_CD || PMT_PAYEE_CD  in ('AC', 'BC', 'CC')                 -- only Cheque records should be counted. Also, ignore reversals (ask Lyn Munz)
  --    and     CAS_PROG_ACTV_CD='N'                                              -- include rows that indicate this payment record did NOT go to CAS (introduced 2018-01 for Bus Pass)
    union all
    --
    --  Client opted for Bus Pass instead of Code 49 Payment (CURR)
    --
    select      /*+ parallel append */ 
                t.FIL_ID_NUM                                                    "MIS_FILE_NUM"
                ,null, null, null, null, null, null, null
                ,0                                                              "CD49_PASS_PREV_BEN_MTH_CNT"
                ,null, null, null, null, case when CAS_BUSPASS_CNT > 0 then 'BP' end, null, null
                ,CAS_BUSPASS_CNT                                                "CD49_PASS_CURR_BEN_MTH_CNT"
    from        mis_stg.TPHIST_FULL_TABLE  t
    inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
    where       PMT_BNFT_MTH_DT >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')
      and       CAS_BUSPASS_CNT > 0
  --    and       PMT_BNFT_MTH_DT >= to_date(20180101,'yyyymmdd')                   -- "TS" started on 2018-Jan Benefit Month
      and       PMT_ISS_CD || PMT_PAYEE_CD  in ('AC', 'BC', 'CC')                 -- only Cheque records should be counted. Also, ignore reversals (ask Lyn Munz)
  --    and     CAS_PROG_ACTV_CD='N'                                              -- include rows that indicate this payment record did NOT go to CAS (introduced 2018-01 for Bus Pass)
  )
  group by    MIS_FILE_NUM
) t
;



-- build temporary table of EA Cases (joined above via Key Player or Spouse)
create table IRSD_BUS_PASS_EA_CP_TMP nologging compress parallel as
select      /*+ parallel append */ distinct
            trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')     "BEN_MTH_DT"
            ,EA.X_LEGACY_FILE_NUM
            ,ea.ROW_WID                                                         "EA_CASE_WID" 
            ,ea.CASE_NUM                                                        "EA_CASE_NUM"
            ,ea.STATUS_CD                                                       "EA_CASE_STATUS_CD"
            ,ea.X_WORK_QUEUE                                                    "EA_WORK_QUEUE"
            ,ea.X_AANDC_ELIG_DT                                                 "EA_AANDC_ELIG_DT"
            ,ea.X_CLASS_CD
            ,EA.X_CLASS_START_DT
--
            ,kp.ROW_WID                                                         "KP_CONTACT_WID"
            ,kp.X_CONTACT_NUM                                                   "KP_CONTACT_NUM"
            ,kp.DECEASE_FLG                                                     "KP_DECEASE_FLG"
            ,round(months_between(trunc(sysdate),kp.BIRTH_DT)/12,2)             "KP_AGE"
            ,case when kp.BIRTH_DT < to_date(to_char(sysdate,'yyyy')-66||'1231','yyyymmdd') then 'Year after 65' end "KP_65_LAST_YEAR_CD"
            ,case 
              when substr(kp.X_PER_STATUS,1,1)='H' then 'PWD' 
              when substr(kp.X_PER_STATUS,1,1) in ('B','O','R') 
                and kp.X_PWD_STAT_CD='Eligible' then 'PWD' 
              else 'not PWD' end                                                "KP_PERSON_PWD_CD"
            ,kp.X_PER_STATUS                                                    "KP_PER_STATUS"
            ,kp.X_PWD_STAT_CD                                                   "KP_PDW_STAT_CD"
            ,kp.X_PWD_ADJUD_DT                                                  "KP_PDW_ADJUD_DT"
            ,kp.X_MSO_RSN_CD                                                    "KP_MSO_RSN_CD"
            ,kp.X_MSO_REVIEW_DT                                                 "KP_MSO_REVIEW_DT"
            ,kp.X_NXT_MON_BP_FLG                                                "KP_NXT_MON_BP_FLG"
            ,kp.X_ACTIVE_BP_FLG                                                 "KP_ACTIVE_BP_FLG"
            ,kp.X_ABOR_BAND_OU_ID                                               "KP_ABOR_BAND_ID"
            ,case when ea.X_CLASS_CD='08 Medical Only' 
              and EA.X_CLASS_START_DT <= to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd')
              then case kp.X_MSO_RSN_CD
                when 'LEAVING-FED BENS AT AGE 65+' then 'MSO (Federal Benefits)'
                when 'TF AEE ZERO' then 'MSO (AEE Exhausted)'
                when 'AEE ZERO' then 'MSO (AEE Exhausted)'
                else 'MSO (other)' end 
                else 'Non-MSO' end                                              "KP_MSO_GROUP_CD"
--
            ,bp_kp.BP_CASE_WID                                                  "KP_BP_CASE_WID"
            ,bp_kp.INAC_FLG                                                     "KP_INAC_FLG"
            ,bp_kp.BP_CASE_NUM                                                  "KP_BP_CASE_NUM"
            ,bp_kp.BP_WORK_QUEUE                                                "KP_BP_WORK_QUEUE"
            ,bp_kp.X_BCT_STICKER_FLG                                            "KP_BP_BCT_STICKER_FLG"
            ,bp_kp.TRANSIT_AREA                                                 "KP_BP_TRANSIT_AREA"
            ,bp_kp.TRANSIT_AREA_GRP                                             "KP_BP_TRANSIT_AREA_GRP"
            ,bp_kp.MAX_SERIAL_NUM                                               "KP_BP_MAX_SERIAL_NUM"
            ,bp_kp.VALID_FLG                                                    "KP_BP_VALID_FLG"
            ,bp_kp.APPROVED_DT                                                  "KP_BP_APPROVED_DT"
            ,bp_kp.APPROVED_SERIAL_NUM                                          "KP_BP_APPROVED_SERIAL_NUM"
            ,bp_kp.APPROVED_PRODUCT_CD                                          "KP_BP_APPROVED_PRODUCT_CD"
            ,bp_kp.APPROVED_PRODUCT_AMT                                         "KP_BP_APPROVED_PRODUCT_AMT"
            ,bp_kp.NM_APPROVED_PRODUCT_CD                                       "KP_NM_APPROVED_PRODUCT_CD"
            ,bp_kp.PENDING_DT                                                   "KP_BP_PENDING_DT"
            ,bp_kp.READY_DT                                                     "KP_BP_READY_DT"
            ,bp_kp.CANCELLED_DT                                                 "KP_BP_CANCELLED_DT"
            ,bp_kp.CANCELLED_SERIAL_NUM                                         "KP_BP_CANCELLED_SERIAL_NUM"
            ,bp_kp.CANCELLED_PRODUCT_CD                                         "KP_BP_CANCELLED_PRODUCT_CD"
--
            ,sp.ROW_WID                                                         "SP_CONTACT_WID"
            ,sp.X_CONTACT_NUM                                                   "SP_CONTACT_NUM"
            ,sp.DECEASE_FLG                                                     "SP_DECEASE_FLG"
            ,round(months_between(trunc(sysdate),SP.BIRTH_DT)/12,2)             "SP_AGE"
            ,case when sp.BIRTH_DT < to_date(to_char(sysdate,'yyyy')-66||'1231','yyyymmdd') then 'Year after 65' end "SP_65_LAST_YEAR_CD"
            ,case 
              when substr(sp.X_PER_STATUS,1,1)='H' then 'PWD' 
              when substr(sp.X_PER_STATUS,1,1) in ('B','O','R') 
                and sp.X_PWD_STAT_CD='Eligible' then 'PWD' 
              when sp.X_CONTACT_NUM is null then 'Unspecified'
              else 'not PWD' end                                                "SP_PERSON_PWD_CD"
            ,sp.X_PER_STATUS                                                    "SP_PER_STATUS"
            ,sp.X_PWD_STAT_CD                                                   "SP_PDW_STAT_CD"
            ,sp.X_PWD_ADJUD_DT                                                  "SP_PDW_ADJUD_DT"
            ,sp.X_MSO_RSN_CD                                                    "SP_MSO_RSN_CD"
            ,sp.X_MSO_REVIEW_DT                                                 "SP_MSO_REVIEW_DT"
            ,sp.X_NXT_MON_BP_FLG                                                "SP_NXT_MON_BP_FLG"
            ,sp.X_ACTIVE_BP_FLG                                                 "SP_ACTIVE_BP_FLG"
            ,sp.X_ABOR_BAND_OU_ID                                               "SP_ABOR_BAND_ID"
            ,case when ea.X_CLASS_CD='08 Medical Only' 
              and EA.X_CLASS_START_DT <= to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd')
              then case sp.X_MSO_RSN_CD
                when 'LEAVING-FED BENS AT AGE 65+' then 'MSO (Federal Benefits)'
                when 'TF AEE ZERO' then 'MSO (AEE Exhausted)'
                when 'AEE ZERO' then 'MSO (AEE Exhausted)'
                else 'MSO (other)' end 
                else 'Non-MSO' end                                              "SP_MSO_GROUP_CD"
--
            ,bp_sp.BP_CASE_WID                                                  "SP_BP_CASE_WID"
            ,bp_sp.INAC_FLG                                                     "SP_INAC_FLG"
            ,bp_sp.BP_CASE_NUM                                                  "SP_BP_CASE_NUM"
            ,bp_sp.BP_WORK_QUEUE                                                "SP_BP_WORK_QUEUE"
            ,bp_sp.X_BCT_STICKER_FLG                                            "SP_BP_BCT_STICKER_FLG"
            ,bp_sp.TRANSIT_AREA                                                 "SP_BP_TRANSIT_AREA"
            ,bp_sp.TRANSIT_AREA_GRP                                             "SP_BP_TRANSIT_AREA_GRP"
            ,bp_sp.MAX_SERIAL_NUM                                               "SP_BP_MAX_SERIAL_NUM"
            ,bp_sp.VALID_FLG                                                    "SP_BP_VALID_FLG"
            ,bp_sp.APPROVED_DT                                                  "SP_BP_APPROVED_DT"
            ,bp_sp.APPROVED_SERIAL_NUM                                          "SP_BP_APPROVED_SERIAL_NUM"
            ,bp_sp.APPROVED_PRODUCT_CD                                          "SP_BP_APPROVED_PRODUCT_CD"
            ,bp_sp.APPROVED_PRODUCT_AMT                                         "SP_BP_APPROVED_PRODUCT_AMT"
            ,bp_sp.NM_APPROVED_PRODUCT_CD                                       "SP_NM_APPROVED_PRODUCT_CD"
            ,bp_sp.PENDING_DT                                                   "SP_BP_PENDING_DT"
            ,bp_sp.READY_DT                                                     "SP_BP_READY_DT"
            ,bp_sp.CANCELLED_DT                                                 "SP_BP_CANCELLED_DT"
            ,bp_sp.CANCELLED_SERIAL_NUM                                         "SP_BP_CANCELLED_SERIAL_NUM"
            ,bp_sp.CANCELLED_PRODUCT_CD                                         "SP_BP_CANCELLED_PRODUCT_CD"
--
            ,case when bp_kp.VALID_FLG = 'B' then 1 else 0 end 
              + case when bp_SP.VALID_FLG = 'B' then 1 else 0 end               "BP_VALID_CNT"
--
            ,mis.MIS_FILE_NUM
            ,mis.DA_CLIENT_TYPE_PREV
            ,mis.DA_CLIENT_TYPE_CURR
            ,mis.DA_CLIENT_PREV_PASS_CNT
            ,mis.DA_CLIENT_PREV_CASH_CNT
            ,mis.DA_CLIENT_PREV_CASH_AMT
            ,mis.DA_CLIENT_CURR_PASS_CNT
            ,mis.DA_CLIENT_CURR_CASH_CNT
            ,mis.DA_CLIENT_CURR_CASH_AMT
--
            ,mis.DA_PREV_BEN_MTH_CD
            ,mis.CD24_PREV_BEN_MTH_CNT
            ,mis.CD24_PREV_BEN_MTH_AMT
            ,mis.CD87_PREV_BEN_MTH_CNT
            ,mis.CD87_PREV_BEN_MTH_AMT
            ,mis.CD49_PASS_PREV_BEN_MTH_CNT
            ,mis.CD49_PREV_BEN_MTH_CNT
            ,mis.CD49_PREV_BEN_MTH_AMT
--
            ,mis.DA_CURR_BEN_MTH_CD
            ,mis.CD24_CURR_BEN_MTH_CNT
            ,mis.CD24_CURR_BEN_MTH_AMT
            ,mis.CD87_CURR_BEN_MTH_CNT
            ,mis.CD87_CURR_BEN_MTH_AMT
            ,mis.CD49_PASS_CURR_BEN_MTH_CNT
            ,mis.CD49_CURR_BEN_MTH_CNT
            ,mis.CD49_CURR_BEN_MTH_AMT
--
            ,BUS_PASS_DERIVE_VALUE('Key Player - Bus Pass', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)   "KP_BP_CNT"
            ,BUS_PASS_DERIVE_VALUE('Spouse - Bus Pass', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)       "SP_BP_CNT"
            ,BUS_PASS_DERIVE_VALUE('Key Player - MIS Pass', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)   "KP_MIS_PASS_CNT"
            ,BUS_PASS_DERIVE_VALUE('Spouse - MIS Pass', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)       "SP_MIS_PASS_CNT"
            ,BUS_PASS_DERIVE_VALUE('Key Player - MIS Cash', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)   "KP_MIS_CASH_CNT"
            ,BUS_PASS_DERIVE_VALUE('Spouse - MIS Cash', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)       "SP_MIS_CASH_CNT"
            ,case when sp.ROW_WID is not null then 'Y' else 'N' end                                                                                                                                      "HAS_SPOUSE_FLG"
            ,BUS_PASS_DERIVE_VALUE('Data Exception', bp_kp.VALID_FLG, bp_sp.VALID_FLG, case when sp.ROW_WID is not null then 'Y' end, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT
              ,case when ea.STATUS_CD='Closed' then 'EA Case Closed; ' end
              || case when kp.DECEASE_FLG = 'Y' then 'EA Key Player flagged Deceased; ' end
              || case when sp.DECEASE_FLG = 'Y' then 'EA Spouse flagged Deceased; ' end
              || case when bp_kp.APPROVED_PRODUCT_CD is null and bp_kp.NM_APPROVED_PRODUCT_CD = 'Replacement' then 'Replacement skipped month; ' end
              || case when bp_sp.APPROVED_PRODUCT_CD is null and bp_sp.NM_APPROVED_PRODUCT_CD = 'Replacement' then 'Replacement skipped month; ' end
              )                                                                                                                                                                                           "CDW_EXCEPTION_TXT"
            ,case when ea.STATUS_CD='Closed' then 'EA Case Closed; ' end
              || case when kp.DECEASE_FLG = 'Y' then 'EA Key Player flagged Deceased; ' end
              || case when sp.DECEASE_FLG = 'Y' then 'EA Spouse flagged Deceased; ' end
              || case when bp_kp.APPROVED_PRODUCT_CD is null and bp_kp.NM_APPROVED_PRODUCT_CD = 'Replacement' then 'Replacement skipped month; ' end
              || case when bp_sp.APPROVED_PRODUCT_CD is null and bp_sp.NM_APPROVED_PRODUCT_CD = 'Replacement' then 'Replacement skipped month; ' end                                                      "SOURCE_ISSUES_TXT"
--            
from        ICM_STG.W_CASE_D EA 
inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
inner join  ICM_STG.WC_CASE_PER_XM kp_xm on ea.row_wid=kp_xm.case_wid and kp_xm.DELETE_FLG='N' 
    and     kp_xm.SUBJECT_FLG='Y' and kp_xm.RELATIONSHIP_CD='Key player'
    and     case when kp_xm.END_DATE is null then 'Y' when trunc(kp_xm.END_DATE) = trunc(ea.CASE_CLOSED_DT) then 'Y' end = 'Y'
inner join  ICM_STG.W_PARTY_PER_D kp on kp.ROW_WID=kp_xm.CONTACT_WID and kp.DELETE_FLG='N' 
left join   IRSD_BUS_PASS_O_CP_TMP bp_kp on bp_kp.CONTACT_WID=kp.ROW_WID
left join   ICM_STG.WC_CASE_PER_XM sp_xm on ea.row_wid=sp_xm.case_wid and sp_xm.DELETE_FLG='N' 
    and     sp_xm.SUBJECT_FLG='Y' and sp_xm.RELATIONSHIP_CD='Spouse'
    and     case when sp_xm.END_DATE is null then 'Y' when trunc(sp_xm.END_DATE) = trunc(ea.CASE_CLOSED_DT) then 'Y' end = 'Y'
left join   ICM_STG.W_PARTY_PER_D sp on sp.ROW_WID=sp_xm.CONTACT_WID and sp.DELETE_FLG='N' 
left join   IRSD_BUS_PASS_O_CP_TMP bp_sp on bp_sp.CONTACT_WID=sp.ROW_WID
left join   IRSD_BUS_PASS_MIS_CP_TMP mis on mis.MIS_FILE_NUM=ea.X_LEGACY_FILE_NUM
where       ea.TYPE_CD='Employment and Assistance' 
and         ea.DELETE_FLG='N'
and         case 
              when ea.STATUS_CD = 'Closed' and nvl( trunc(ea.CASE_CLOSED_DT),trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm') ) >= trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm') then 'Y' 
              when ea.STATUS_CD not in ('Closed','Pending','Admin Re-open') then 'Y'
              end = 'Y'
;



-- merge BP and EA and MIS into Archive for reporting
create table IRSD_BUS_PASS_CP_EXCEPTIONS nologging compress as
(
    select      /*+ parallel append */ 'EA' "MATCH_TYPE", t.* from IRSD_BUS_PASS_EA_CP_TMP t
    where       BP_VALID_CNT > 0
    or          DA_CLIENT_TYPE_PREV ||  DA_CLIENT_TYPE_CURR is not null
  union all
    select
              'BP'
              ,trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')  "BEN_MTH_DT"
              ,null,null,null,null,null,null,null,null
  --
              ,CONTACT_WID
              ,X_CONTACT_NUM
              ,DECEASE_FLG
              ,PERSON_AGE
              ,PERSON_65_LAST_YEAR_CD
              ,PERSON_PWD_CD
              ,X_PER_STATUS
              ,X_PWD_STAT_CD
              ,X_PWD_ADJUD_DT
              ,X_MSO_RSN_CD
              ,X_MSO_REVIEW_DT
              ,X_NXT_MON_BP_FLG
              ,X_ACTIVE_BP_FLG
              ,X_ABOR_BAND_OU_ID
              ,'No EA Case'                                                     --KP_MSO_GROUP_CD
  --
              ,BP_CASE_WID
              ,INAC_FLG
              ,BP_CASE_NUM
              ,BP_WORK_QUEUE
              ,X_BCT_STICKER_FLG
              ,TRANSIT_AREA
              ,TRANSIT_AREA_GRP
              ,MAX_SERIAL_NUM
              ,VALID_FLG
              ,APPROVED_DT
              ,APPROVED_SERIAL_NUM
              ,APPROVED_PRODUCT_CD
              ,APPROVED_PRODUCT_AMT
              ,NM_APPROVED_PRODUCT_CD
              ,PENDING_DT
              ,READY_DT
              ,CANCELLED_DT
              ,CANCELLED_SERIAL_NUM
              ,CANCELLED_PRODUCT_CD
  --
              ,null,null,null,null,null,null,null,null,null,null,null,null, null, null
              ,'No EA Case'                                                     --SP_MSO_GROUP_CD
              ,null,null,null,null,null,null,null,null,null,null,null,null, null
              , null, null, null, null, null, null
  --
              ,case when VALID_FLG = 'B' then 1 else 0 end                      "BP_VALID_CNT"
  --
              ,null,null,null,null,null,null,null,null,null
              ,null,null,null,null,null,null,null,null
              ,null,null,null,null,null,null,null,null
              ,case when VALID_FLG = 'B' then '1' else '0' end                  "KP_BP_CNT"
              ,null
              ,null
              ,null
              ,null
              ,null
              ,'N'
              ,to_char(null)
              ,to_char(null)
  --
    from        IRSD_BUS_PASS_O_CP_TMP o
    inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
    left join   IRSD_BUS_PASS_EA_CP_TMP kp on BP_CASE_WID = kp.KP_BP_CASE_WID 
    left join   IRSD_BUS_PASS_EA_CP_TMP sp on BP_CASE_WID = sp.SP_BP_CASE_WID 
    where       kp.KP_BP_CASE_WID || sp.SP_BP_CASE_WID is null
    and         VALID_FLG = 'B' 
  union all
    select
              'MIS'
              ,trunc(to_date(substr(m.ASOF_DATE_NAME,1,10),'yyyy-mm-dd'),'mm')  "BEN_MTH_DT"
              ,null,null,null,null,null,null,null,null
  --
              ,null,null,null,null,null,null,null,null,null,null,null,null, null, null
              ,'No EA Case'                                                     --KP_MSO_GROUP_CD
              ,null,null,null,null,null,null,null,null,null,null,null,null, null
              , null, null, null, null, null, null
  --
              ,null,null,null,null,null,null,null,null,null,null,null,null, null, null
              ,'No EA Case'                                                     --SP_MSO_GROUP_CD
              ,null,null,null,null,null,null,null,null,null,null,null,null, null
              , null, null, null, null, null, null
  --
              ,null                                                             "BP_VALID_CNT"
  --
              ,mis.MIS_FILE_NUM
              ,mis.DA_CLIENT_TYPE_PREV
              ,mis.DA_CLIENT_TYPE_CURR
              ,mis.DA_CLIENT_PREV_PASS_CNT
              ,mis.DA_CLIENT_PREV_CASH_CNT
              ,mis.DA_CLIENT_PREV_CASH_AMT
              ,mis.DA_CLIENT_CURR_PASS_CNT
              ,mis.DA_CLIENT_CURR_CASH_CNT
              ,mis.DA_CLIENT_CURR_CASH_AMT
  --
              ,mis.DA_PREV_BEN_MTH_CD
              ,mis.CD24_PREV_BEN_MTH_CNT
              ,mis.CD24_PREV_BEN_MTH_AMT
              ,mis.CD87_PREV_BEN_MTH_CNT
              ,mis.CD87_PREV_BEN_MTH_AMT
              ,mis.CD49_PASS_PREV_BEN_MTH_CNT
              ,mis.CD49_PREV_BEN_MTH_CNT
              ,mis.CD49_PREV_BEN_MTH_AMT
  --
              ,mis.DA_CURR_BEN_MTH_CD
              ,mis.CD24_CURR_BEN_MTH_CNT
              ,mis.CD24_CURR_BEN_MTH_AMT
              ,mis.CD87_CURR_BEN_MTH_CNT
              ,mis.CD87_CURR_BEN_MTH_AMT
              ,mis.CD49_PASS_CURR_BEN_MTH_CNT
              ,mis.CD49_CURR_BEN_MTH_CNT
              ,mis.CD49_CURR_BEN_MTH_AMT
  --
              ,null
              ,null
              ,BUS_PASS_DERIVE_VALUE('Key Player - MIS Pass', null, null, null, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)   "KP_MIS_PASS_CNT"
              ,BUS_PASS_DERIVE_VALUE('Spouse - MIS Pass', null, null, null, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)       "SP_MIS_PASS_CNT"
              ,BUS_PASS_DERIVE_VALUE('Key Player - MIS Cash', null, null, null, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)   "KP_MIS_CASH_CNT"
              ,BUS_PASS_DERIVE_VALUE('Spouse - MIS Cash', null, null, null, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)       "SP_MIS_CASH_CNT"
              ,'N'                                                                                                                          "HAS_SPOUSE_FLG"
              ,BUS_PASS_DERIVE_VALUE('Data Exception', null, null, null, mis.DA_CLIENT_CURR_PASS_CNT, mis.DA_CLIENT_CURR_CASH_CNT)          "CDW_EXCEPTION_TXT"
              ,to_char(null)
  --
    from        IRSD_BUS_PASS_MIS_CP_TMP mis
    inner join  CDW.CDW_ASOF_DATES m on SOURCE_SYSTEM_ID='iRSD'
    left join   IRSD_BUS_PASS_EA_CP_TMP ea on ea.MIS_FILE_NUM = mis.MIS_FILE_NUM
    where       ea.MIS_FILE_NUM is null
    and         mis.DA_CLIENT_TYPE_PREV ||  mis.DA_CLIENT_TYPE_CURR is not null
);
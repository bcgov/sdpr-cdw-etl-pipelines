select
    FTE_Sum.CLNDR_MTH_ID,
    PAY_PERIODS_CNT.FSCL_PERIOD_ID,
    -- performing manual conversion to EP format
    CASE substr(to_char(PAY_PERIODS_CNT.FSCL_PERIOD_ID),5,2)
        WHEN '01' THEN 'Apr'
        WHEN '02' THEN 'May'
        WHEN '03' THEN 'Jun'
        WHEN '04' THEN 'Jul'
        WHEN '05' THEN 'Aug'
        WHEN '06' THEN 'Sep'
        WHEN '07' THEN 'Oct'
        WHEN '08' THEN 'Nov'
        WHEN '09' THEN 'Dec'
        WHEN '10' THEN 'Jan'
        WHEN '11' THEN 'Feb'
        WHEN '12' THEN 'Mar'
        ELSE 'XXX'              -- unexpected value
    END || ' '  ||
        substr (to_char(to_number(substr(to_char(PAY_PERIODS_CNT.FSCL_PERIOD_ID),1,4))-1),3,2) ||
        '-' || substr(to_char(PAY_PERIODS_CNT.FSCL_PERIOD_ID),3,2)
    as FSCL_PERIOD_NAM,
    FTE_Sum.DEPTID as BU_DEPTID,
    FTE_Sum.RESP_NUM,
    FTE_Sum.FTE_REG,
    FTE_Sum.FTE_OVT,
    FTE_Sum.FIRE_OVT,
    FTE_Sum.FTE_BURN ,
    PAY_PERIODS_CNT.NUM_PAY_PERIODS ,
    -- Calculate Average FTE burn per fiscal pay period
    --(month may have 2 or 3 pay periods, FTE_BURN is not additive)
    FTE_Sum.FTE_BURN /PAY_PERIODS_CNT.NUM_PAY_PERIODS as  EFP_AVG_FTE
from (
    select
        --pay_end_dt,
        to_number(to_char(pay_end_dt,'YYYYMM')) as CLNDR_MTH_ID,
        deptid,
        tgb_gl_response as RESP_NUM,
        sum(fte_reg) as  fte_reg,
        sum(fte_ovt) as  fte_ovt,
        sum(fire_ovt) as fire_ovt,
        sum(fte_reg+fte_ovt+fire_ovt) as fte_burn
    from ps_tgb_fteburn_tbl
    where pay_end_dt>=to_date('20060401','YYYYMMDD')
        and business_unit ='BC031'
    group by to_number(to_char(pay_end_dt,'YYYYMM')), deptid, tgb_gl_response
    --order by 1,2,3
) FTE_Sum
INNER JOIN (
    select
        clndr_pay_month ,
        to_number(clndr_pay_month) as clndr_mth_id,
        case
            when substr(clndr_pay_month,5,2) <='03' then to_number(clndr_pay_month) + 9   -- Jan, Feb, Mar
            else ((to_number(substr(clndr_pay_month,1,4))+1)*100) + to_number(substr(clndr_pay_month,5,2))-3
        end as FSCL_PERIOD_ID,
        count(*) as  NUM_PAY_PERIODS
    from (
        select distinct pay_end_dt, to_char(pay_end_dt,'YYYYMM') as clndr_pay_month 
        from ps_pay_calendar
    )
    where clndr_pay_month>='200601'
    group by clndr_pay_month,
        case
            when substr(clndr_pay_month,5,2) <='03'  then to_number(clndr_pay_month) + 9
            else (to_number(substr(clndr_pay_month,1,4))+1)*100 + to_number(substr(clndr_pay_month,5,2))-3
        end
) PAY_PERIODS_CNT
    ON PAY_PERIODS_CNT.clndr_mth_id=FTE_Sum.CLNDR_MTH_ID
;
Create Table ODS.IRSD_MYSS_PLMS_USERS AS  
with 

org_users as ( 
    SELECT IDIR_AT_PAY_END leaf_idir, 
        e.pay_end_dt,
        nvl(
            lead(e.pay_end_dt) over (partition by e.IDIR_AT_PAY_END order by e.pay_end_dt), 
            to_date('99981231','yyyymmdd')
        ) next_pay_end_dt, 
        leaf_rc  
    FROM EM_ORGANIZATION_employee e   
    WHERE IDIR_AT_PAY_END is not null 
        and IDIR_AT_PAY_END <> 'Unspecified'
        and e.pay_end_dt >= to_date('20170218', 'yyyymmdd')   /* only activity plans after feb 22 included*/ 
) 

SELECT  /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
    e.leaf_idir, 
    e.pay_end_dt, 
    e.next_pay_end_dt, 
    d.resp_l3_ID_AND_nam, 
    SUBSTR(d.resp_l3_ID_AND_nam, 1, 5) l3_rc 
FROM org_users e  
join cdw.or_responsibility_d d 
    on e.pay_end_dt >= to_date('20170218', 'yyyymmdd')   /* only activity plans after feb 22 included*/ 
        AND substr(e.leaf_rc, 1, 2) = '46'                       /* only our Ministry RCs*/ 
        AND e.leaf_rc = d.resp_num 
        and d.fscl_yr = to_number(to_char(add_months(e.pay_end_dt, 9), 'yyyy'))  /* get hierarchy for fiscal year of pay record*/
        and SUBSTR(d.resp_l3_ID_AND_nam, 1, 5) = '46C33'
;

commit;
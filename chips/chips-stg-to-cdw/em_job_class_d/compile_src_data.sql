select
    jc.setid||jc.jobcode jobcode_bk,
    jc.setid,
    jc.jobcode,
    jc.effdt,
    jc.eff_status jc_eff_status,
    jc.descr jc_descr,
    jc.descrshort jc_descrshort,
    jc.sal_admin_plan,
    jc.grade,
    jc.step,
    jc.union_cd,
    jc.std_hours,
    jc.std_hrs_frequency,
    jc.job_function,
    case
        when (
            select x.xlatlongname
            from psxlatitem x
            where jc.job_function = x.fieldvalue
                and x.fieldname = 'TGB_JOB_FUNCTION'
                and x.effdt = (
                    select max(xx.effdt)
                    from psxlatitem xx
                    where xx.fieldname = x.fieldname
                        and xx.fieldvalue = x.fieldvalue
                        and xx.effdt <= jc.effdt
                )
        ) is not null
            then (
                select x.xlatlongname from psxlatitem x
                where jc.job_function = x.fieldvalue
                    and x.fieldname = 'TGB_JOB_FUNCTION'
                    and x.effdt = (
                        select max(xx.effdt)
                        from psxlatitem xx
                        where xx.fieldname = x.fieldname
                            and xx.fieldvalue = x.fieldvalue
                            and xx.effdt <= jc.effdt
                    )
            )
        else jc.job_function
    end job_func_descr,
    substr(jc.job_function,1,2) emp_group,
    DECODE (
        SUBSTR (jc.job_function, 1, 2),
        '11', 'BCGEU',
        '12', 'PEA',
        '13', 'NURSES',
        '14', 'GCIU',
        '04', 'MGMT',
        '05', 'OIC PSA',
        '06', 'SAL PHY',
        '07', 'OTHER',
        '08', 'NON PSA OIC ABC',
        '09', 'NON PSA',
        null
   ) emp_grp_descr,
    substr(jc.job_function,1,1) incl_excl,
    DECODE (
        SUBSTR(jc.job_function,1,1),
        '1','Included',
	    '0','Excluded',
	    null
   ) incl_excl_descr
from ps_jobcode_tbl jc
where jc.effdt >= (
    select nvl(max(to_date(x2.effdt,'yyyy-mm-dd hh24:mi:ss')),
        to_date('19400101','yyyymmdd'))
    from cdw.em_job_class_d x2
    where jc.setid = x2.setid
        and jc.jobcode = x2.jobcode
)
ORDER BY jc.setid,jc.jobcode,jc.effdt
;
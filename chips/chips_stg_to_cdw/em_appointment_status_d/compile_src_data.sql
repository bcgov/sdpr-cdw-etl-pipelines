SELECT
    DISTINCT j.empl_ctg as appointment_status,
    x.descr as appt_status_descr,
    x.descrshort as appt_descr_short,
    DECODE(
        j.empl_ctg,
        'K','A',
        'L','A',
        'M','A',
        'U','A',
        j.empl_ctg
    ) appointment_group,
    DECODE(
        j.empl_ctg,
        'K','Auxiliary',
        'L','Auxiliary',
        'M','Auxiliary',
        'U','Auxiliary',
        x.descr 
    ) appt_group_descr,
    DECODE(
        j.empl_ctg,
        'K','Aux',
        'L','Aux',
        'M','Aux',
        'U','Aux',
        x.descrshort 
    ) appt_group_descr_short
FROM ps_job j, (
    SELECT
        x.LABOR_AGREEMENT,
        x.empl_ctg ,
        x.effdt,
        x.descr,
        x.descrshort
    FROM ps_empl_ctg_l1 x
    WHERE
        --      X.LABOR_AGREEMENT='GOV'  AND
        x.effdt = (SELECT MAX(x2.effdt) FROM  ps_empl_ctg_l1 x2 WHERE x.empl_ctg = x2.empl_ctg
        and x.effdt<sysdate
        --    and X.LABOR_AGREEMENT='GOV'
    )
) x
WHERE j.empl_ctg = x.empl_ctg(+)
;
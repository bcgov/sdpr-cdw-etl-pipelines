SELECT
	x.fieldvalue empl_status,
	to_char(x.effdt,'yyyy-mm-dd hh24:mi:ss') effdt,
	x.xlatlongname descr,
	DECODE(
        x.fieldvalue,
        'A','All Active',
        'L','All Active',
        'P','All Active',
        'S','All Active',
        'D','All Non-Active',
        'R','All Non-Active',
        'T','All Non-Active',
        'UNKNOWN'
    ) status_grp
FROM psxlatitem x
WHERE x.fieldname = 'EMPL_STATUS'
    AND x.eff_status = 'A'
	AND x.effdt = (
        SELECT MAX(x2.effdt) 
        FROM psxlatitem  x2
        WHERE x2.fieldname = x.fieldname
            AND x2.fieldvalue = x.fieldvalue
            AND x2.effdt <= SYSDATE
    )
;
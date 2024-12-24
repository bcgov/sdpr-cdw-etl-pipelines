SELECT l.COUNTRY,
    COUNTRY_CODE,
    decode(
        TRIM(STATE), NULL, 'BC',
        'C','BC',
        TRIM(STATE)
    ) STATE,
    c.city,
    l.setid||l.location setid_loc,
    l.setid,
    LOCATION,
    DESCR,
    ADDRESS1,
    ADDRESS2,
    ADDRESS3,
    ADDRESS4,
    POSTAL,
    x.xlatlongname regional_district
FROM PS_LOCATION_TBL l,
    ps_TGB_city_tbl c,
    psxlatitem x
WHERE UPPER(l.city) = UPPER(c.city)
    AND c.tgb_reg_district = x.fieldvalue
    AND x.fieldname = 'TGB_REG_DISTRICT'
    AND x.eff_status = 'A'
    AND x.effdt = (
        SELECT MAX(x2.effdt)
        FROM psxlatitem  x2
        WHERE x.fieldname = x2.fieldname
            AND x.fieldvalue = x2.fieldvalue
            AND x2.eff_status = x.eff_status
            AND x2.effdt <= SYSDATE
    )
    AND l.SETID = 'BCSET'
    AND l.EFF_STATUS = 'A'
    AND l.effdt = (
        SELECT MAX(l2.effdt)
        FROM PS_LOCATION_TBL l2
        WHERE l.setid = l2.setid
            AND l.location = l2.location
            AND l.eff_status = l2.eff_status
            AND l2.effdt <= SYSDATE
    )
;
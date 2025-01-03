SELECT /*+RULE*/
    p.position_nbr,
    p.eff_status,
    p.descr position_descr,
    p.descrshort,
    trim(p.can_noc_cd||'-'||p.tgb_can_noc_sub_cd) sub_noc_cd,
    SUBNOC_TBL.SUB_descr,
    p.can_noc_cd,
    NOC_TBL.NOC_DESCR,
    p.budgeted_posn,
    p.key_position,
    p.reports_to,
    p.report_dotted_line
FROM (
    SELECT DISTINCT
    cn.can_noc_cd,  CN.DESCR NOC,
    n.tgb_can_noc_sub_cd,   n.descr sub_descr
    from ps_tgb_cnocsub_tbl n,
        ps_can_noc_tbl cn
    WHERE cn.effdt = (
        select max(cn2.effdt)
        from ps_can_noc_tbl cn2
        where cn2.can_noc_cd = cn.can_noc_cd
            and cn2.effdt <=sysdate
    )
        and n.can_noc_cd = cn.can_noc_cd
        and n.effdt = (
            select max(n2.effdt)
            from ps_tgb_cnocsub_tbl n2
            where n2.can_noc_cd = cn.can_noc_cd
                and n.tgb_can_noc_sub_cd=n2.tgb_can_noc_sub_cd
                and n2.effdt <=sysdate
        )
) SUBNOC_TBL,
(
    SELECT DISTINCT cn.can_noc_cd,  CN.DESCR NOC_DESCR
    from ps_can_noc_tbl cn
    WHERE cn.effdt = (
        select max(cn2.effdt)
        from ps_can_noc_tbl cn2
        where cn2.can_noc_cd = cn.can_noc_cd
            and cn2.effdt <= sysdate
    )
) NOC_TBL,
ps_position_data p
WHERE p.can_noc_cd = SUBNOC_TBL.can_noc_cd(+)
    AND p.tgb_can_noc_sub_cd = SUBNOC_TBL.tgb_can_noc_sub_cd(+)
    AND p.can_noc_cd = NOC_TBL.can_noc_cd(+)
    AND p.effdt = (
        SELECT MAX(p2.effdt)
        FROM ps_position_data p2
        WHERE p2.position_nbr = p.position_nbr
            AND p2.effdt <= SYSDATE
    )
;
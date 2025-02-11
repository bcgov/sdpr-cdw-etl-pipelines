SELECT 
    PX.SETID || PX.TREE_NODE as BU_BK,
    Leaf.descr as BU_Descr,
    Leaf.deptid as BU_deptid,
    Leaf.company as Company,
    CMP.Descr as Company_Descr,
    CMP.Descrshort as Company_Descr_Short,
    PX.TREE_NAME,
    greatest(PX.EFFDT, leaf.effdt) as Tree_Date, -- Eff_DATE
    PX.SETID,
    NVL2(px.L1_TREE_NODE,PX.setid || px.L1_TREE_NODE, NULL) as L1_BK,
    L1_TREE_NODE as L1_deptid,
    L1_TREE_NODE_NUM,
    'BUS_UNIT' as L1_Name,
    L1.descr as L1_Descr,
    L1.descrshort as L1_DescrShort,
    L1.effdt as L1_effdt,
    NVL2(px.L2_TREE_NODE, PX.setid || px.L2_TREE_NODE, NULL) as L2_BK,
    L2_TREE_NODE as L2_deptid,
    L2_TREE_NODE_NUM,
    'PROGRAM' as L2_Name,
    L2.descr as L2_Descr,
    L2.descrshort as L2_DescrShort,
    L2.effdt as L2_effdt,
    NVL2(px.L3_TREE_NODE, PX.setid || px.L3_TREE_NODE, NULL) as L3_BK,
    L3_TREE_NODE as L3_deptid,
    L3_TREE_NODE_NUM,
    'DIVISION' as L3_Name,
    L3.descr as L3_Descr,
    L3.descrshort as L3_DescrShort,
    L3.effdt as L3_effdt,
    NVL2(px.L4_TREE_NODE, PX.setid || px.L4_TREE_NODE, NULL) as L4_BK,
    L4_TREE_NODE as L4_deptid,
    L4_TREE_NODE_NUM,
    'BRANCH' as L4_Name,
    L4.descr as L4_Descr,
    L4.descrshort as L4_DescrShort,
    L4.effdt as L4_effdt,
    NVL2(px.L5_TREE_NODE, PX.setid || px.L5_TREE_NODE, NULL) as L5_BK,
    L5_TREE_NODE as L5_deptid,
    L5_TREE_NODE_NUM,
    'SECTION' as L5_Name,
    L5.descr as L5_Descr,
    L5.descrshort as L5_DescrShort,
    L5.effdt as L5_effdt,
    NVL2(px.L6_TREE_NODE, PX.setid || px.L6_TREE_NODE, NULL) as L6_BK,
    L6_TREE_NODE as L6_deptid,
    L6_TREE_NODE_NUM,
    'UNIT' as L6_Name,
    L6.descr as L6_Descr,
    L6.descrshort as L6_DescrShort,
    L6.effdt as L6_effdt,
    NVL2(px.L7_TREE_NODE, PX.setid || px.L7_TREE_NODE, NULL) as L7_BK,
    L7_TREE_NODE as L7_deptid,
    L7_TREE_NODE_NUM,
    'DEPARTMENT' as L7_Name,
    L7.descr as L7_Descr,
    L7.descrshort as L7_DescrShort,
    L7.effdt as L7_effdt,
    Leaf.TGB_GL_CLIENT as GL_CLIENT,
    Leaf.TGB_GL_RESPONSE as GL_RESPONSE,
    Leaf.TGB_GL_SERVICE_LN as GL_SERVICE_LN,
    Leaf.TGB_GL_PROJECT as GL_PROJECT,
    Leaf.TGB_GL_STOB as GL_STOB
FROM "PX_TREE_FLATTENED" PX

LEFT JOIN (
    select setid, deptid, descr, descrshort, company,effdt,
        TGB_GL_CLIENT, TGB_GL_RESPONSE, TGB_GL_SERVICE_LN, TGB_GL_PROJECT, TGB_GL_STOB
    /* omit future dated changes for Department and company records. */
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid = d2.setid 
            and d1.deptid = d2.deptid
            and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
        )
) LEAF
    on PX.setid=Leaf.setid 
        and PX.tree_node=Leaf.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt 
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid=d2.setid 
            and d1.deptid=d2.deptid
            and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L1 
    on PX.setid=L1.setid 
        and PX.L1_tree_node=L1.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid = d2.setid 
            and d1.deptid = d2.deptid
            and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L2 
    on PX.setid = L2.setid 
        and PX.L2_tree_node = L2.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid=d2.setid 
            and d1.deptid=d2.deptid
		    and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L3 
    on PX.setid = L3.setid 
        and PX.L3_tree_node = L3.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid=d2.setid 
            and d1.deptid=d2.deptid
		    and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L4 
    on PX.setid = L4.setid 
        and PX.L4_tree_node = L4.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt 
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid = d2.setid 
            and d1.deptid = d2.deptid
		    and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L5 
    on PX.setid = L5.setid 
        and PX.L5_tree_node = L5.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid = d2.setid 
            and d1.deptid = d2.deptid
		    and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L6 
    on PX.setid = L6.setid 
        and PX.L6_tree_node = L6.deptid

LEFT JOIN (
    select setid, deptid, descr, descrshort, effdt
    from ps_dept_tbl d1
    where effdt = (
        select max(effdt) 
        from ps_dept_tbl d2 
        where d1.setid=d2.setid 
            and d1.deptid=d2.deptid
		    and d2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) L7 
    on PX.setid = L7.setid 
        and PX.L7_tree_node = L7.deptid

LEFT JOIN (
    select company, descr, descrshort 
    from ps_company_tbl t1
    where effdt = (
        select max(effdt)
        from ps_company_tbl t2 
        where t1.company=t2.company 
            and t2.effdt <= (
                select pay_period_end_date 
                from cdw.chips_load_control 
                where load_in_progress_ind = 1
            )
    )
) CMP 
    on Leaf.company=CMP.company

LEFT JOIN (
    select max(eff_date) eff_date,d.bu_bk
    from cdw.or_business_unit_d d 
    group by d.bu_bk 
) BU
    on PX.SETID || PX.TREE_NODE = bu.bu_bk

WHERE PX.TREE_NAME='DEPT_SECURITY'
    and PX.SETID not in ('QEGID', 'COMMN','ST000')
    -- filter for QEGID added in June 2013 ; this started causing issues (NULL value attributes) after PeopleSoft upgrade
    and PX.SETID like 'ST%'
    -- filter updated Sept 2017; database move started causing issues (NULL GL value attributes) after PeopleSoft upgrade with new SetIDs found in PreProd DB
    -- found extra eets in the pstreenode table which did not join ot the ps dept tbl and resulted in null values for non null GL columns
    -- May 25th TSS  only select the latest row from the PX file to be added to the SCD
    and PX.EFFDT = (
        select max(PX2.EFFDT) 
        from "PX_TREE_FLATTENED" PX2 
        where px2.setid = px.setid 
            and px2.tree_node = px.tree_node
    )
    /* May 25th TSS getting many rows coming into the process, as department changes occuring after effective date of dataset. */
    -- and PX.EFFDT >= nvl( bu.eff_Date,to_date('19000101','yyyymmdd'))
    and greatest(PX.EFFDT, leaf.effdt) >= nvl(bu.eff_Date,to_date('19000101', 'yyyymmdd'))
order by PX.SETID || PX.TREE_NODE, greatest(PX.EFFDT, leaf.effdt)
;
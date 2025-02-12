with

pay_period_end_date_load_control as (
    select pay_period_end_date 
    from cdw.chips_load_control 
    where load_in_progress_ind = 1
),

ranked_depts as (
    select dept.*,
        row_number() over (partition by setid, deptid order by effdt desc) as rn
    from chips_stg.ps_dept_tbl dept
),

latest_eff_dt_by_dept as (
    select *
    from ranked_depts
    where rn = 1
),

load_latest_eff_dt_by_dept as (
    select * 
    from latest_eff_dt_by_dept
    where effdt <= (
        select * 
        from pay_period_end_date_load_control
    )
),

ranked_companies AS (
    select co.*,
        row_number() over (partition by company order by effdt desc) as rn
    from chips_stg.ps_company_tbl co
),

latest_eff_dt_by_company as (
    select *
    from ranked_companies
    where rn = 1
),

load_latest_eff_dt_by_company as (
    select * 
    from latest_eff_dt_by_company
    where effdt <= (
        select * 
        from pay_period_end_date_load_control
    )
),

level_query as (
    select setid, deptid, descr, descrshort, effdt 
    from load_latest_eff_dt_by_dept
)

select 
    px.setid || px.tree_node as bu_bk,
    leaf.descr as bu_descr,
    leaf.deptid as bu_deptid,
    leaf.company as company,
    cmp.descr as company_descr,
    cmp.descrshort as company_descr_short,
    px.tree_name,
    greatest(px.effdt, leaf.effdt) as tree_date, -- eff_date
    px.setid,
    nvl2(px.l1_tree_node,px.setid || px.l1_tree_node, null) as l1_bk,
    l1_tree_node as l1_deptid,
    l1_tree_node_num,
    'BUS_UNIT' as l1_name,
    l1.descr as l1_descr,
    l1.descrshort as l1_descrshort,
    l1.effdt as l1_effdt,
    nvl2(px.l2_tree_node, px.setid || px.l2_tree_node, null) as l2_bk,
    l2_tree_node as l2_deptid,
    l2_tree_node_num,
    'PROGRAM' as l2_name,
    l2.descr as l2_descr,
    l2.descrshort as l2_descrshort,
    l2.effdt as l2_effdt,
    nvl2(px.l3_tree_node, px.setid || px.l3_tree_node, null) as l3_bk,
    l3_tree_node as l3_deptid,
    l3_tree_node_num,
    'DIVISION' as l3_name,
    l3.descr as l3_descr,
    l3.descrshort as l3_descrshort,
    l3.effdt as l3_effdt,
    nvl2(px.l4_tree_node, px.setid || px.l4_tree_node, null) as l4_bk,
    l4_tree_node as l4_deptid,
    l4_tree_node_num,
    'BRANCH' as l4_name,
    l4.descr as l4_descr,
    l4.descrshort as l4_descrshort,
    l4.effdt as l4_effdt,
    nvl2(px.l5_tree_node, px.setid || px.l5_tree_node, null) as l5_bk,
    l5_tree_node as l5_deptid,
    l5_tree_node_num,
    'SECTION' as l5_name,
    l5.descr as l5_descr,
    l5.descrshort as l5_descrshort,
    l5.effdt as l5_effdt,
    nvl2(px.l6_tree_node, px.setid || px.l6_tree_node, null) as l6_bk,
    l6_tree_node as l6_deptid,
    l6_tree_node_num,
    'UNIT' as l6_name,
    l6.descr as l6_descr,
    l6.descrshort as l6_descrshort,
    l6.effdt as l6_effdt,
    nvl2(px.l7_tree_node, px.setid || px.l7_tree_node, null) as l7_bk,
    l7_tree_node as l7_deptid,
    l7_tree_node_num,
    'DEPARTMENT' as l7_name,
    l7.descr as l7_descr,
    l7.descrshort as l7_descrshort,
    l7.effdt as l7_effdt,
    leaf.tgb_gl_client as gl_client,
    leaf.tgb_gl_response as gl_response,
    leaf.tgb_gl_service_ln as gl_service_ln,
    leaf.tgb_gl_project as gl_project,
    leaf.tgb_gl_stob as gl_stob
from chips_stg.px_tree_flattened px

left join (
    select setid, deptid, descr, descrshort, company, effdt,
        tgb_gl_client, tgb_gl_response, tgb_gl_service_ln, tgb_gl_project, tgb_gl_stob
    from load_latest_eff_dt_by_dept
) leaf
    on px.setid=leaf.setid 
        and px.tree_node=leaf.deptid

left join (
    select * from level_query
) l1 
    on px.setid=l1.setid 
        and px.l1_tree_node=l1.deptid

left join (
    select * from level_query
) l2 
    on px.setid = l2.setid 
        and px.l2_tree_node = l2.deptid

left join (
    select * from level_query
) l3 
    on px.setid = l3.setid 
        and px.l3_tree_node = l3.deptid

left join (
    select * from level_query
) l4 
    on px.setid = l4.setid 
        and px.l4_tree_node = l4.deptid

left join (
    select * from level_query
) l5 
    on px.setid = l5.setid 
        and px.l5_tree_node = l5.deptid

left join (
    select * from level_query
) l6 
    on px.setid = l6.setid 
        and px.l6_tree_node = l6.deptid

left join (
    select * from level_query
) l7 
    on px.setid = l7.setid 
        and px.l7_tree_node = l7.deptid

left join (
    select company, descr, descrshort 
    from load_latest_eff_dt_by_company
) cmp 
    on leaf.company=cmp.company

left join (
    select max(eff_date) eff_date,d.bu_bk
    from cdw.or_business_unit_d d 
    group by d.bu_bk 
) bu
    on px.setid || px.tree_node = bu.bu_bk

where px.tree_name = 'DEPT_SECURITY'
    and px.setid not in ('QEGID', 'COMMN','ST000')
    and px.setid like 'ST%'
    -- only select the latest row from the PX file to be added to the SCD
    and px.effdt = (
        select max(px2.effdt) 
        from chips_stg.px_tree_flattened px2 
        where px2.setid = px.setid 
            and px2.tree_node = px.tree_node
    )
    and greatest(px.effdt, leaf.effdt) >= nvl(bu.eff_date, to_date('19000101', 'yyyymmdd'))
order by px.setid || px.tree_node, greatest(px.effdt, leaf.effdt)
;
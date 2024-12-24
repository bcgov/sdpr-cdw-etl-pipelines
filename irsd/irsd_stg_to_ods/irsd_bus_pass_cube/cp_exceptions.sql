merge into irsd_bus_pass_cube target using (
    select 	/*parallel 4 */	 t.BEN_MTH_DT, 
        t.KP_BP_CASE_WID, 
        t.SP_BP_CASE_WID, 
        t.EA_CASE_WID, 
        t.MIS_FILE_NUM, 
        t.cdw_exception_txt,  
        t.source_issues_txt  
    from irsd_bus_pass_cp_exceptions t 
    where t.cdw_exception_txt is not null 
        or t.source_issues_txt is not null  
) src on (
    target.BEN_MTH_DT = src.BEN_MTH_DT 
    and NVL(target.KP_BP_CASE_WID,0) = NVL(src.KP_BP_CASE_WID,0)  
    and NVL(target.SP_BP_CASE_WID,0) = NVL(src.SP_BP_CASE_WID,0) 
    and NVL(target.EA_CASE_WID,0) = NVL(src.EA_CASE_WID,0) 
    and NVL(target.MIS_FILE_NUM,'0') = NVL(src.MIS_FILE_NUM,'0') 
) 
when matched then Update 
set target.cp_exception = nvl(src.cdw_exception_txt, 'Unspecified'), 
    target.cp_src_exception = nvl(src.source_issues_txt, 'Unspecified')
;
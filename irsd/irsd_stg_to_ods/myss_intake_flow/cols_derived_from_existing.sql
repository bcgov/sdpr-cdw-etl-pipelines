MERGE INTO IRSD_MYSS_INTAKE_FLOW TARGET 
USING (  
    select /*+ PARALLEL 4 DYNAMIC_SAMPLING(4)*/ 
        pKey, 
        Case 
            when ia_x_created_by in ('SIEBEL_EAI_MSD','SADMIN','SIEBEL_EAI', 'HSDGENUSR')  
                then 'System' 
            when ia_x_created_by is NULL then '( Blank )' 
            else 'Worker'
            end Application_Created_by,
        Case  
            when ia_status in ('Closed','Cancelled') 
                and ia_resolution_cd = 'Cancelled via MYSS' 
                then 'Withdrawn by Client' 
            when ia_status in ('Closed') 
                and  ia_resolution_cd = 'Withdrawn' 
                and ia_x_owner in ('SADMIN','HSDGENUSR') 
                and application_submission_dt is null 
                then 'System' 
            when ia_status in ('Cancelled') then 'Worker' 
            when ia_close_dt is NULL then '( Blank )' 
            when ia_x_owner in ('SADMIN','HSDGENUSR') then 'System' 
            else 'Worker'
            end Application_Closed_By,
        Case 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                and ia_x_sub_sub_type = 'Expedited' 
                and ia_x_comm_method_cd != 'Interpretation Services'
                then 'Expedited' 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                and ia_x_sub_sub_type = 'Expedited'
                and ia_x_comm_method_cd = 'Interpretation Services'
                then 'Expedited Interpreter' 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                and ia_x_sub_sub_type = 'Fleeing Abuse'
                then 'Fleeing Abuse' 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                then 'General' 
            when (ia_x_svc_office like '0IF%' 
                or ia_x_svc_office like '0IH%' 
                or ia_x_svc_office like '0IP%' 
                or ia_x_svc_office like '0IC%') 
                then 'Specialized' 
            when ia_sr_wid is NULL then '( Blank )'  
            else 'Other'
            end Application_Grouping,
        Case 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                and ia_x_sub_sub_type = 'Expedited'
                then 'Expedited' 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                and ia_x_sub_sub_type = 'Fleeing Abuse' 
                then 'Fleeing Abuse' 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                and ia_x_sub_sub_type = 'Interpreter Required'
                then 'Interpreter Required' 
            when (ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%' 
                or ia_x_svc_office like '099%') 
                then 'General' 
            when (ia_x_svc_office like '0IF%' 
                or ia_x_svc_office like '0IH%' 
                or ia_x_svc_office like '0IP%' 
                or ia_x_svc_office like '0IC%') 
                then 'Specialized' 
            when ia_sr_wid is NULL then '( Blank )'  
            else 'Other'
            end New_Application_Grouping,
        Case 
            when Application_submission_dt is null 
            then 'FALSE' 
            else 'TRUE'  
            end Application_Submitted,
        Case 
            when ia_x_owner is null then 'FALSE' 
            else 'TRUE' 
            end Application_Owned,
        case 
            when ia_x_svc_office like '0IA%' 
                or ia_x_svc_office like '0IB%' 
                or ia_x_svc_office like '0IN%' 
                or ia_x_svc_office like '0CP%'  
                or ia_x_svc_office like '099%' 
                then 'General Intake' 
            when ia_x_svc_office like '0IF%' 
                or ia_x_svc_office like '0IH%' 
                or ia_x_svc_office like '0IP%' 
                or ia_x_svc_office like '0IC%' 
                then 'Specialized Intake' 
            when ia_x_svc_office is null then '( Blank )' 
            else 'Other' 
            end Service_Office_Group
    from irsd_myss_intake_flow i  
) SRC ON (TARGET.pKey = SRC.pKey) 
WHEN MATCHED THEN UPDATE 
    SET  
    TARGET.Application_Created_by = SRC.Application_Created_by, 
    TARGET.Application_Closed_By = SRC.Application_Closed_By, 
    TARGET.Application_Grouping = SRC.Application_Grouping, 
    TARGET.New_Application_Grouping = SRC.New_Application_Grouping, 
    TARGET.Application_Submitted = SRC.Application_Submitted, 
    TARGET.Application_Owned = SRC.Application_Owned, 
    TARGET.Service_Office_Group = SRC.Service_Office_Group
;

commit;
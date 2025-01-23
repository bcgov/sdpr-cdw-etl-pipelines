select distinct 
    EA_Case_details.Case_Number Case_Number, 
    EA_Case_details.Case_Type_code Case_Type_code, 
    Extended_Health_Enrollment.MSP_Enrollment_ID MSP_Enrollment_ID, 
    Contact_details.Contact_ID___ICM Contact_ID___ICM, 
    EA_Case_details.File__legacy__number File__legacy__number, 
    EA_Case_details.Case_Status_code Case_Status_code
from (
    (
        (
            select IRSD_CASE_CONTACT_XM.CASE_WID CASE_WID, 
                IRSD_CASE_CONTACT_XM.CONTACT_WID CONTACT_WID
            from ODS.IRSD_CASE_CONTACT_XM IRSD_CASE_CONTACT_XM
            where IRSD_CASE_CONTACT_XM.CASE_TYPE='Employment and Assistance' 
                and IRSD_CASE_CONTACT_XM.D_VALID_FLG='Y'
        ) Case_Contact 
        INNER JOIN (
            select IRSD_CONTACT_F.X_CONTACT_NUM Contact_ID___ICM, 
                IRSD_CONTACT_F.CONTACT_WID CONTACT_WID
            from CDW.IRSD_CONTACT_F IRSD_CONTACT_F
        ) Contact_details 
            on Case_Contact.CONTACT_WID=Contact_details.CONTACT_WID
    ) 
    INNER JOIN (
        select IRSD_CASE_D.CASE_NUM Case_Number, 
            IRSD_CASE_D.STATUS_CD Case_Status_code, 
            IRSD_CASE_D.TYPE_CD Case_Type_code, 
            IRSD_CASE_D.X_LEGACY_FILE_NUM File__legacy__number, 
            IRSD_CASE_D.CASE_WID CASE_WID
        from CDW.IRSD_CASE_D IRSD_CASE_D
        where IRSD_CASE_D.TYPE_CD='Employment and Assistance'
    ) EA_Case_details 
        on Case_Contact.CASE_WID=EA_Case_details.CASE_WID
) 
LEFT OUTER JOIN (
    select IRSD_EXTENDED_HEALTH_D.MSP_ENROLLMENT_ID MSP_Enrollment_ID, 
        IRSD_EXTENDED_HEALTH_D.CASE_WID CASE_WID, 
        IRSD_EXTENDED_HEALTH_D.CONTACT_WID CONTACT_WID
    from CDW.IRSD_EXTENDED_HEALTH_D IRSD_EXTENDED_HEALTH_D, 
        CDW.IRSD_EH_DIVISION_D IRSD_EH_DIVISION_D, 
        CDW.IRSD_EH_CLASS_D IRSD_EH_CLASS_D
    where IRSD_EXTENDED_HEALTH_D.CLASS=IRSD_EH_CLASS_D.EH_CLASS_CODE 
        and IRSD_EXTENDED_HEALTH_D.DIVISION=IRSD_EH_DIVISION_D.EH_DIVISION_CODE
) Extended_Health_Enrollment 
    on Case_Contact.CONTACT_WID=Extended_Health_Enrollment.CONTACT_WID 
    and Case_Contact.CASE_WID=Extended_Health_Enrollment.CASE_WID
where EA_Case_details.Case_Status_code in ('Open', 'Pending', 'Admin Re-open')
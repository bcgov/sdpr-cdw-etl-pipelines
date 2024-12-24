SELECT
	LEVEL5_DEPTID as DEPT_ID ,
	BU_SID,
       BU_DEPTID,
	BU_BK,
       5 AS HIER_LEVEL
FROM   "CDW"."OR_BUSINESS_UNIT_D"
WHERE CURR_IND = 'Y'
 AND SETID = 'ST031'
 AND LEVEL5_DEPTID IS NOT NULL
;
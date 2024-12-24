UPDATE EM_POSITION_D SET
	"POSITION_NBR" = ORCHESTRATE."POSITION_NBR",
	"POSITION_DESCR" = ORCHESTRATE."POSITION_DESCR",
	"EFF_STATUS" = ORCHESTRATE."EFF_STATUS",
	"DESCRSHORT" = ORCHESTRATE."DESCRSHORT",
	"BUDGETED_POSN" = ORCHESTRATE."BUDGETED_POSN",
	"KEY_POSITION" = ORCHESTRATE."KEY_POSITION",
	"REPORTS_TO" = ORCHESTRATE."REPORTS_TO",
	"REPORT_DOTTED_LINE" = ORCHESTRATE."REPORT_DOTTED_LINE",
	"NOC_SUB_CD" = ORCHESTRATE."NOC_SUB_CD",
	"SUBCODE" = ORCHESTRATE."SUBCODE",
	"CAN_NOC_CD" = ORCHESTRATE."CAN_NOC_CD",
	"NOC" = ORCHESTRATE."NOC",
	"END_DATE" = ORCHESTRATE."END_DATE",
	"UDT_DATE" = ORCHESTRATE."DSJobStartTimestamp",
	"CURR_IND" = ORCHESTRATE."CURR_IND"
WHERE "POSITION_SID" = ORCHESTRATE."POSITION_SID"
;
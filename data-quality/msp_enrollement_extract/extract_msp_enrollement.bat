@ECHO OFF
@SETLOCAL & PUSHD & SET RET=
@REM ---------------------------------------------------------------
@REM HCDWLPPA - populate SDD related ODS tables from ICM_STG.
@REM
@REM This job loads the Pillar tables from ICM_STG into ODS and CDW
@REM for Service Delivery Division. 

@SET JOB_NAME=%~n0
@SET JOB_DESCRIPTION=Extract (case_num, enrollment_id) for DQ.
@SET OBJ_TYPE=Data
@SET APP_SYS=DQ

@CALL %ETL_BIN%\EnvironmentStart.bat
@IF %RET% NEQ 0 GOTO EXIT

:Job_Initiation

@CD /D %APP_SH%

:Job_Execution

@REM activate virtual environment
call E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\.venv\Scripts\activate.bat

@REM run python job script
python "E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\extract_msp_enrollement.py"

@echo python done

@SET EXIT_CODE=%RET%
	echo EXIT_CODE is %EXIT_CODE% >>%BATCH_LOG_FILE%
	echo RET is %RET% >>%BATCH_LOG_FILE%

if %RET%==0 (
	echo [%TIME%] %~n0 finished -- SUCCESS -- >>%BATCH_LOG_FILE% 
) 
else (
	echo [%TIME%] %~n0 finished -- FAILURE -- >>%BATCH_LOG_FILE% 
)

@CALL %ETL_BIN%\EnvironmentEnd.bat

@POPD & ENDLOCAL & SET EXIT_CODE=%EXIT_CODE%  & SET AGENT_EXE=%AGENT_EXE%

@REM Send Return code back to ESP.
%AGENT_EXE% %EXIT_CODE%

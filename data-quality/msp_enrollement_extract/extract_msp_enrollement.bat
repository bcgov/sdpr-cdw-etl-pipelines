@REM activate virtual environment
call E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\.venv\Scripts\activate.bat

@REM run python job script
@REM cmd has form: python [script that performs the extraction] [sql query to use to extract data] [path to output xlsx file]
python "E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\extract_msp_enrollement.py"
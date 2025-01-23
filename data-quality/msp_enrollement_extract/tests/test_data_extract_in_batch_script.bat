@REM activate virtual environment
call E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\.venv\Scripts\activate.bat

@REM run python job script
@REM cmd has form: python [script that performs the extraction] [sql query to use to extract data] [path to output xlsx file]
python "E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\data_extract_cmd_line.py" E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\tests\sample.sql E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\tests\data.xlsx
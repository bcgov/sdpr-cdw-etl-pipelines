@REM activate virtual environment
call E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\.venv\Scripts\activate.bat

@REM run python job script
python "E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\data_extract_cmd_line.py" ^
    E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\sample.sql ^ 
    E:\ETL_V8\sdpr-cdw-data-pipelines\data-extract\data.xlsx
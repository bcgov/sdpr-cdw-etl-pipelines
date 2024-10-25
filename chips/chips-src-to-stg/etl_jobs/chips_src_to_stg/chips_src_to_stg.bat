@REM activate virtual environment
call E:\ETL_V8\sdpr-cdw-data-pipelines\chips\chips-src-to-stg\.venv\Scripts\activate.bat

@REM run python job script
python "E:\ETL_V8\sdpr-cdw-data-pipelines\chips\chips-src-to-stg\etl_jobs\chips_src_to_stg\chips_src_to_stg.py"
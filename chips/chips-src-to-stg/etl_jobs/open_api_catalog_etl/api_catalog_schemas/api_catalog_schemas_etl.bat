@REM activate virtual environment
call E:\ETL_V8\sdpr-cdw-data-pipelines\chips\chips-src-to-stg\.venv\Scripts\activate.bat

@REM run python job script
python "E:\ETL_V8\sdpr-cdw-data-pipelines\chips\chips-src-to-stg\etl_jobs\open_api_catalog_etl\api_catalog_schemas_etl.py"
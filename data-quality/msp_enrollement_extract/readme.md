# HCDWLPEN Operations Doc
This job: 
1. extracts data from the Oracle SDPR CDW via an SQL query,
2. packages the data as an xlsx file, and 
3. delivers it to business users via a shared folder.

## Order of Key Events
Relative to the base directory, `E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract`:
1. ESP runs `hcdwlpen.bat`, which
2. runs `extract_mas_enrollment.py`, which
    1. runs `dq_msp_enrollment.sql` against the Oracle SDPR CDW
    2. packages the returned data as an xlsx file
    3. delivers the xlsx file to `//sfp.idir.bcgov/s134/s34404/GetDoc/CDW-SDPR/DQ TL/MSP Enrollment ID by Case Number Report.xlsx`

## Schedule
* Frequency: Every business day
* Time: 6am
* Dependant on: HCDWLPPA and HCDWLPPW, i.e. the tables that are referenced in this sql file: `E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\dq_msp_enrollment.sql`
* Successor Job(s): None
* Approx run time: 1 min
* Condition Code: 
    * 0 := successful completion
    * non-zero := failure

## Preparation
None

## Manual Execution
This job can be manually executed by running `E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\hcdwlpen.bat`.

## Restarting
If the job fails, contact the SDPR CDW team (James Scott, Keith Davies, SDSI.BISupport@gov.bc.ca) to resolve the issue unless the failure is on ESP's side. Then you can just rerun it.

## Checking
If the job runs successfully, it will return a zero code.

If the job issues any other condition (other than 0), a notification is sent to SDSI.BISupport@gov.bc.ca for action.

Contents of the Log File(s) referenced below may be reviewed for more information on the job.

## File Maintenance
None

## File Distribution
creates/overwrites: `//sfp.idir.bcgov/s134/s34404/GetDoc/CDW-SDPR/DQ TL/MSP Enrollment ID by Case Number Report.xlsx`

## Reference

Batch Job Dir
* `E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\hcdwlpen.bat`

Documentation Dir
* `E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract\readme.md`

Log File Dir
* `E:\ETL_V8\Test\DQ\log\hcdwlpen`

Source Code Dir
* `E:\ETL_V8\sdpr-cdw-data-pipelines\data-quality\msp_enrollement_extract`


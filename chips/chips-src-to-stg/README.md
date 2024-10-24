# MHRGRP PeopleSoft API ETL Documentation

## Following Initial Clone

- Rename `config_template.yml` to `config.yml` and set the configuration values
- Rename `SDPR_keypair_template.pem` to `SDPR_keypair.pem` and add the pem key
- Rename `template.env` to `.env` and set the environment variable values

Setup your virtual environment to install an independent set of python packages in the project directory in the .venv folder:
1. create a venv in the root directory: `python -m venv .venv`
2. activate the venv in powershell terminal: `.venv\Scripts\Activate.ps1`

Install packages in your virtual environment:
* Offline (if on the servers):
  1. open a Windows Command Prompt
  2. activate the venv: `.venv\Scripts\activate.bat`
  3. go into the folder: `cd venv_downloads`
  4. install all packages: `for %x in (dir *.whl) do python -m pip install %x`
* Online (if on your own machine): `pip install -r requirements.txt`

## Managing Libraries
Generate a requirements.txt file containing all libraries installed in the venv by running: `pip3 freeze > requirements.txt` so you can install required packages in new venvs by running `pip install -r requirements.txt`. 

Since our ETL servers require offline installs, I created a folder called `venv_downloads` to house all of the `.whl` files, which can be installed by following the instructions on offline package installs above.

To refresh the venv_downloads folder based on the current `requirements.txt` file, you need to be on a machine that let's you access the internet so you can run pip installations. From such a machine:
1. create a new folder
2. `cd [new folder]`
3. `pip download -r ../requirements.txt`
4. delete all .whl files in venv_downloads
5. paste all the .whl files downloaded in the new folder in venv_downloads
6. delete the new folder

Now, you can merge these changes made on your local machine into the main branch, pull the changes on the servers, and do an offline install on the servers.

## Formatting
Ruff is used for formatting. You can run `format ruff` in powershell from the root directory to format all code. You can configure formatting rules in the `ruff.toml` and `pyproject.toml` files. See the ruff documentation online for details.

## Source Control
Use Git + Azure DevOps for source control:
1. create a development branch off the main branch: `git checkout -b your-branch-name`
2. publish the branch to the remote: `git push -u origin your-branch-name`
3. stage all changes: `git add .`
4. commit staged changes: `git commit -m "your commit message"`
5. push changes: `git push`
6. go to the associated Azure DevOps repo and create a pull request. Assign reviewers for code 
reviews.

To pull changes from the remote, run `git pull`

## Generating Documentation with Sphinx
Open bash terminal
1. `source .venv/Scripts/activate`
2. `cd docs`
3. auto-generate the .rst files: `sphinx-apidoc -o ./source ../[relative path to folder containing the new modules, i.e. src] -f --separate`
4. `make html`
5. The documentation website is located at: `docs\build\html\index.html`. Just paste the full file path in your browser.

## Running the HCDWLPWA Job
The HCDWLPWA job retrieves HR data from the MHRGRP API, loads it into the CHIPS_STG staging area in Oracle, and then builds analytical tables in the CDW schema from the staging tables. 

### `HCDWLPWA.bat`
Run this batch script to refresh CHIPS_STG tables and some CDW and ODS tables that are built using CHIPS_STG tables.

The HCDWLPWA job is run according to the HCDWLPWA.bat file located on the ETL Servers at E:\ETL_V8\prod\chips\shellscript\HCDWLPWA.bat.

The HCDWLPWA.bat file runs the CHIPS_ETL_STEPS.bat file, which is in the same folder, to do the ETL work.

### `CHIPS_ETL_STEPS.bat`
The first step in CHIPS_ETL_STEPS.bat gets data from the MHRGRP API and loads it into CHIPS_STG in the Oracle CDW. The only transformations that occur at this step are (should be) data type transformations required to load the JSON values retrieved from the API into the corresponding Oracle CDW columns. The following code in CHIPS_ETL_STEPS.bat activates the python virtual environment and runs the python code in E:\ETL_V8\Python\peoplesoft-etl-pipeline\HCDWLPWA.py:


```
@REM activate virtual environment
call E:\ETL_V8\Python\peoplesoft-etl-pipeline\.venv\Scripts\activate.bat
@REM run python job script
python "E:\ETL_V8\Python\peoplesoft-etl-pipeline\etl_jobs\peoplesoft_src_to_stg\peoplesoft_src_to_stg.py"
```

### `peoplesoft_src_to_stg.py`
This script can be run to build the staging tables in CHIPS_STG. These are the tables that the analytical tables in the CDW are built upon; this script does not build the analytical tables. 

When this script is run, it first calls the build_tables function to build the tables:


```
build_tables(
    endpoint_table_pairs = endpoint_table_pairs,
    n_task_workers = 10,
    start_task_sleep_time = 2,
)
```
The argument n_task_workers is used to set the number of tasks that can be run concurrently using a queue worker. The argument start_task_sleep_time is used to set the minimum number of seconds between tasks starts. If too many tasks start to close together, then the API will receive more requests than it can handle for a time period and return HTTP 429 errors.

If you examine the build_tables function (where it is defined, as in, def build_tables), you will see this list of endpoint-table name pairs for each table that is to be built:


```
# API endpoint-Oracle table pairs
endpoint_table_pairs = [
    # Rebuild entire table
    ("ps_earnings_tbl", "PS_EARNINGS_TBL"),
    ("ps_empl_ctg_l1", "PS_EMPL_CTG_L1"),
    ("ps_pay_calendar", "PS_PAY_CALENDAR"),
    ("ps_pay_oth_earns_pay_dates", "PS_PAY_OTH_EARNS_PAY_DATES"),
    ("ps_sal_plan_tbl", "PS_SAL_PLAN_TBL"),
    ("ps_union_tbl", "PS_UNION_TBL"),
    ("ps_setid_tbl", "PS_SETID_TBL"),
    ("ps_tgb_city_tbl", "PS_TGB_CITY_TBL"),
    ("ps_tgb_cnocsub_tbl", "PS_TGB_CNOCSUB_TBL"),
    ("ps_sal_grade_tbl", "PS_SAL_GRADE_TBL"),
    ("ps_bus_unit_tbl_hr", "PS_BUS_UNIT_TBL_HR"),
    ("ps_action_tbl", "PS_ACTION_TBL"),
    ("ps_actn_reason_tbl", "PS_ACTN_REASON_TBL"),
    ("ps_can_noc_tbl", "PS_CAN_NOC_TBL"),
    ("ps_company_tbl", "PS_COMPANY_TBL"),
    ("ps_deduction_class", "PS_DEDUCTION_CLASS"),
    ("ps_deduction_tbl", "PS_DEDUCTION_TBL"),
    ("ps_jobcode_tbl", "PS_JOBCODE_TBL"),
    ("ps_location_tbl", "PS_LOCATION_TBL"),
    ("ps_sal_step_tbl", "PS_SAL_STEP_TBL"),
    ("treedefn", "TREEDEFN"),
    ("pstreelevel", "PSTREELEVEL"),
    ("psxlatitem", "PSXLATITEM"),
    ("ps_dept_tbl", "PS_DEPT_TBL"),
    ("psoprdefn_bc", "PS_OPRDEFN_BC_TBL"),
    ("ps_employees", "PS_EMPLOYEES"),
    ("ps_employment", "PS_EMPLOYMENT"),
    ("ps_personal_data", "PS_PERSONAL_DATA"),
    ("ps_set_cntrl_rec", "PS_SET_CNTRL_REC"),
    ("ps_position_data", "PS_POSITION_DATA"),
    ("pstreenode", "PSTREENODE"),
    ("ps_job", "PS_JOB"),

    # Upsert recently created records for large tables
    ("ps_tgb_fteburn_tbl_by_date", "PS_TGB_FTEBURN_TBL"),
    ("ps_pay_check_by_date", "PS_PAY_CHECK"),
    ("ps_pay_oth_earns_by_date", "PS_PAY_OTH_EARNS"),
    ("ps_pay_earnings_by_date", "PS_PAY_EARNINGS"),
]
```
The function iterates through each pair to build/update the staging tables in CHIPS_STG using the function, etl_engine.run_etl_worker (Remember, you can drill into functions using ctrl+click in a good code editor, like VS Code.) Comment out lines for tables that you don’t want to build/re-build, if you like. etl_engine.run_etl_worker runs a worker that allows multiple ETL tasks to be run concurrently according to the helper function async_etl_task. async_etl_task gets the data from the API endpoint, applies data type transformations, and loads the data into the corresponding CDW Oracle table in CHIPS_STG.
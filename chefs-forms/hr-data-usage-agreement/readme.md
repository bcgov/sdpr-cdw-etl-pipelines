# ETL for the HR Data Governance Agreement Submission Data
- Data source: CHEFS API
- Data destination: SDPR Oracle DB

### ETL Pipeline
1. An SDPR employee submits an HR Data Governance Agreement CHEFS form.
2. The data is retrieved from the CHEFS API, which is thouroughly documented on the internet by the CHEFS development team.
3. Form submission data is loaded into the SDPR Oracle Database into the `etl.hr_data_usage_agreement_submissions` table for SDPR HR Data Owners to analyze.

### Requirements to run this code
- Access to the SDPR KeePass file and password.
- The KeePass file is expected to be located at: `S:\Info Tech\Operations - Applications (6820)\Local appl (by name) (6820-30)\Corporate Data Warehouse\Cognos 11 and Data Stage\Data Stage\Credentials.kdbx`.
    * If it's not on your computer, then you need to update the path in src.oracle_db. Just search the code for `kdbx_path`.
- The `config.yml` file. See `config-template.yml`.

### Setting Up Your Virtual Environment
Do this (in powershell or similar) after you clone the repo:
1. create a venv in the current directory: `python -m venv .venv`
2. activate the venv: `.venv\Scripts\Activate.ps1`
3. install packages in requirements.txt by running: `pip install -r requirements.txt`
4. sidenote: you can update requirements.txt if you install more packages: `pip3 freeze > requirements.txt`
- This was developed using Python version 3.12.1

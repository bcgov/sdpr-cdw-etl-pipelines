import pandas as pd
from dotenv import load_dotenv
import yaml
import os
import logging
import sys
import oracledb
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
main_base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(main_base_dir)
from utils.oracle_db import OracleDB

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

# get config file
with open(base_dir + '\\' + 'config.yml', 'r') as file:
    conf = yaml.safe_load(file)

# extract data from excel file
df = pd.read_excel('chips\chips-src-to-stg\oracle_ddl\create_primary_keys\mhrgrp_api_primary_keys.xlsx')
pkeys_df = df[['SYNONYM', 'Primary Index']]

# add primary keys to chips_stg tables
chips_stg_db = OracleDB(conn_str_key_endpoint=conf['chips_stg_conn_str_subkey'])

for index, row in pkeys_df.iterrows():
    endpoint_name = row['SYNONYM'].strip()
    pk_fields = row['Primary Index']
    if not pd.isnull(pk_fields):
        try:
            chips_stg_db.add_primary_key(
                table_owner='CHIPS_STG',
                table_name=endpoint_name,
                pk_name=f'{endpoint_name}_PK',
                pk_cols=pk_fields,
            )
        except oracledb.DatabaseError as e:
            error, = e.args
            if error.code == 942:
                print('Ignored ORA-00942: table or view does not exist')

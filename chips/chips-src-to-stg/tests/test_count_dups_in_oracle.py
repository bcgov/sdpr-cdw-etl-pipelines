import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
from src.oracle_db import OracleDB
import yaml

with open(base_dir + '\\' + 'config.yml', 'r') as file:
    conf = yaml.safe_load(file)

etl_db = OracleDB(conn_str_key_endpoint=conf['etl_conn_str_subkey'])

print('start')
duplicates_in_oracle = etl_db.count_dups_in_oracle(
    table_owner='CHIPS_STG', table_name='PS_PAY_EARNINGS'
)
print('end')
print(duplicates_in_oracle)

print('start')
duplicates_in_oracle = etl_db.count_dups_in_oracle(
    table_owner='CHIPS_STG', table_name='PS_PAY_OTH_EARNS'
)
print('end')
print(duplicates_in_oracle)

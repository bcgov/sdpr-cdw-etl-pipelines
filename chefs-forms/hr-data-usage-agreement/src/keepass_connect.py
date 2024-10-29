from pykeepass import PyKeePass
from getpass import getpass
import oracledb

kdbx_path = r'S:\Info Tech\Operations - Applications (6820)\Local appl (by name) (6820-30)\Corporate Data Warehouse\Cognos 11 and Data Stage\Data Stage\Credentials.kdbx'

# CW1D ETL
cw1d_etl_connection_name='cw1d_etl'
cw1d_etl_group_name='SDPR CDW (CW1D)'
cw1d_etl_entry_title='ETL'

# CW1T2 ETL
cw1t2_etl_connection_name='cw1t2_etl'
cw1t2_etl_group_name='SDPR CDW (CW1T2)'
cw1t2_etl_entry_title='ETL'

# CW1P ETL
cw1p_etl_connection_name='cw1p_etl'
cw1p_etl_group_name='SDPR CDW (CW1P)'
cw1p_etl_entry_title='ETL'

def get_conn_str(kdbx_path: str, group_name: str=cw1d_etl_group_name, entry_title: str=cw1d_etl_entry_title):
    kdbx_path = kdbx_path
    keepass = PyKeePass(kdbx_path, password=getpass("Enter KeePass password: "))
    group = keepass.find_groups(name=group_name, first=True)
    entry = keepass.find_entries(title=entry_title, group=group, first=True)
    dsn = f'{entry.username}/{entry.password}@{entry.url}'
    return dsn

def connect_to_ora(dsn: str):
    return oracledb.connect(dsn)
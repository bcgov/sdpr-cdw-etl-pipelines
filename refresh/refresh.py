import logging
import sys
from dotenv import load_dotenv
import os 
load_dotenv(dotenv_path="E:\\ETL_V8\\sdpr-cdw-data-pipelines\\refresh\\.env")
base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(base_dir)
from utils.sql_plus import SqlPlus
from utils.cmd import Cmd

this_dir = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)
logging.basicConfig(
    # filename=f'{this_dir}\\refresh.log',
    # filemode='w',
    level=logging.DEBUG, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)


class Refresh:

    def __init__(self):
        pass

    def import_table_w_datapump(
        self,
        tables,
        remap_from_table=None,
        remap_to_table=None,
        to_db_user=os.getenv('TO_DB_USER'),
        to_db_password=os.getenv('TO_DB_PASSWORD'),
        to_db_connect_identifier=os.getenv('TO_DB_CONNECT_IDENTIFIER'),
        dir=os.getenv('DATAPUMP_ENV_REFRESH_DIR'),
        network_link=os.getenv('FROM_DB_LINK'),
        table_exists_action='replace',
    ):
        # impdp command fragments
        _destination_db = f'{to_db_user}/{to_db_password}@{to_db_connect_identifier}'
        _directory = f'directory={dir}'
        _network_link = f'network_link={network_link}'
        _tables = f'tables={tables}'
        if remap_from_table is None and remap_to_table is None:
            _remap = ''
        else:
            _remap = f'remap_table={remap_from_table}:{remap_to_table}'
        _table_exists_action = f'table_exists_action={table_exists_action}'

        cmd = f'impdp "{_destination_db}" {_directory} {_network_link} {_tables} {_remap} {_table_exists_action}'
        logger.info(cmd)
        c = Cmd()
        c.run_cmd(cmd)

    def refresh_table_w_sqlplus(self, table):
        s = SqlPlus()
        s.create_or_replace_copy(
            from_db_user = os.getenv('FROM_DB_USER'),
            from_db_password = os.getenv('FROM_DB_PASSWORD'),
            from_db_connect_identifier = os.getenv('FROM_DB_CONNECT_IDENTIFIER'),
            to_db_user = os.getenv('TO_DB_USER'),
            to_db_password = os.getenv('TO_DB_PASSWORD'),
            to_db_connect_identifier = os.getenv('TO_DB_CONNECT_IDENTIFIER'),
            target_table=table,
            destination_table=table,
        )

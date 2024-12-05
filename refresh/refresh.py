import logging
import sys
from dotenv import load_dotenv
import os 
load_dotenv(dotenv_path="E:\\ETL_V8\\sdpr-cdw-data-pipelines\\refresh\\.env")
base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(base_dir)
from utils.sql_plus import SqlPlus

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

    def full_refresh(self, table):
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

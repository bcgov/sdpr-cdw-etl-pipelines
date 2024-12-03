import logging
import sys
from dotenv import load_dotenv
import os 
load_dotenv()
base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(base_dir)
from utils.oracle_db import OracleDB
from utils.data_extractor import DataExtractor

this_dir = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=f'{this_dir}\data_extract.log',
    filemode='w',
    level=logging.DEBUG, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

if __name__ == "__main__":
    try:
        db = OracleDB(conn_str_key_endpoint='CW1D_ETL')
        data_extractor = DataExtractor(oracle_db=db)
        data_extractor.sql_to_xlsx(sys.argv[1], sys.argv[2])
    except:
        logging.exception('Got exception on main handler')
        raise

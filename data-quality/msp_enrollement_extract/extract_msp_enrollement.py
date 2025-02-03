import logging
import sys
from dotenv import load_dotenv
import os 
load_dotenv()
base_dir = os.getenv('MAIN_BASE_DIR')
esp_log_file_dir = os.getenv('ESP_LOG_FILE_DIR')
sys.path.append(base_dir)
from utils.oracle_db import OracleDB
from utils.data_extractor import DataExtractor

this_dir = os.path.dirname(os.path.realpath(__file__))

log_filepath = fr'{this_dir}\extract_msp_enrollement.log'
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=log_filepath,
    filemode='w',
    level=logging.DEBUG, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

sql_file_path = fr'{this_dir}\dq_msp_enrollment.sql'
xlsx_file_path = r'//sfp.idir.bcgov/s134/s34404/GetDoc/CDW-SDPR/DQ TL/MSP Enrollment ID by Case Number Report.xlsx'
xlsx_dir = r'//sfp.idir.bcgov/s134/s34404/GetDoc/CDW-SDPR/DQ TL/'
xlsx_filename_before_timestamp = r'MSP Enrollment ID by Case Number Report'

if __name__ == "__main__":
    try:
        db = OracleDB(conn_str_key_endpoint=os.getenv('ORACLE_CONN_STRING_KEY'))
        data_extractor = DataExtractor(oracle_db=db)
        data_extractor.sql_to_xlsx_with_timestamp(
            sql_file_path, 
            xlsx_dir, 
            xlsx_filename_before_timestamp,
            delete_old_timestamped_xlsx_files=True,
        )
        logger.info(fr'finished the sql to xlsx extraction from {sql_file_path} to {xlsx_file_path}')
    except:
        logging.exception('Got exception on main handler')
        sys.exit(1)
    sys.exit(0)

import logging
import sys
from dotenv import load_dotenv
import os 
load_dotenv(dotenv_path="E:\\ETL_V8\\sdpr-cdw-data-pipelines\\refresh\\.env")
base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(base_dir)
from refresh.refresh import Refresh

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

def main():
    tables = '''
        cas_stg.AP_IEXPENSE_ATTACHMENTS,
        cas_stg.AP_IEXPENSE_ATTACHMENTS2,
        cas_stg.AP_INVOICES,
        cas_stg.AP_INVOICES_WRK,
        cas_stg.AP_INVOICE_BATCHES,
        cas_stg.AP_INVOICE_BATCHES_WRK,
        cas_stg.AP_INVOICE_PAYMENTS,
        cas_stg.AP_INVOICE_PAYMENTS_WRK,
        cas_stg.AP_PAYMENTS,
        cas_stg.AP_PAYMENTS2,
        cas_stg.AP_PAYMENTS_STG,
        cas_stg.AP_PAYMENT_DISTRIBUTIONS,
        cas_stg.AP_PAYMENT_DISTRIBUTIONS2,
        cas_stg.AP_TRAVEL_DELEGATE_ASSIGNMENTS,
        cas_stg.AP_TRAVEL_DISTRIBUTIONS,
        cas_stg.AP_TRAVEL_DISTRIBUTIONS2,
        cas_stg.AP_TRAVEL_GROUPS,
        cas_stg.AP_TRAVEL_HEADERS,
        cas_stg.AP_TRAVEL_HEADERS2,
        cas_stg.AP_TRAVEL_RATE_HISTORY,
        cas_stg.AP_VENDORS,
        cas_stg.AP_VENDORS_STG,
        cas_stg.AP_VENDOR_HISTORY,
        cas_stg.AP_VENDOR_HISTORY2,
        cas_stg.AP_VENDOR_SITES,
        cas_stg.AP_VENDOR_SITES_STG,
        cas_stg.BUDGET_NAMES,
        cas_stg.BUDGET_NAMES2,
        cas_stg.EMPLOYEES,
        cas_stg.EMPLOYEES2,
        cas_stg.EMPLOYEE_HISTORY,
        cas_stg.EMPLOYEE_HISTORY2,
        cas_stg.FA_ASSETS,
        cas_stg.FA_ASSETS2,
        cas_stg.FA_ASSET_DISTRIBUTIONS,
        cas_stg.FA_ASSET_DISTRIBUTIONS2,
        cas_stg.GL_ANNUAL_BUDGETS,
        cas_stg.GL_ANNUAL_BUDGETS2,
        cas_stg.GL_AP_PO_DETAIL,
        cas_stg.GL_AP_PO_DETAIL2,
        cas_stg.GL_BALANCES_DETAIL,
        cas_stg.GL_BALANCES_DETAIL2,
        cas_stg.GL_BALANCES_DETAIL_TEMP,
        cas_stg.GL_JE_BATCHES,
        cas_stg.GL_JE_BATCHES2,
        cas_stg.GL_JE_HEADERS,
        cas_stg.GL_JE_HEADERS2,
        cas_stg.GL_JE_LINES,
        cas_stg.GL_ROLLUP_CLIENT,
        cas_stg.GL_ROLLUP_CLIENT2,
        cas_stg.GL_ROLLUP_DETAIL,
        cas_stg.GL_ROLLUP_DETAIL2,
        cas_stg.GL_ROLLUP_PROJECT,
        cas_stg.GL_ROLLUP_PROJECT2,
        cas_stg.GL_ROLLUP_RESPONSIBILITY,
        cas_stg.GL_ROLLUP_RESPONSIBILITY2,
        cas_stg.GL_ROLLUP_SERVICE_LINE,
        cas_stg.GL_ROLLUP_SERVICE_LINE2,
        cas_stg.GL_ROLLUP_STOB,
        cas_stg.GL_ROLLUP_STOB2,
        cas_stg.IRSD_IMMEFT_RECONCILE_CAS,
        cas_stg.PERIODS,
        cas_stg.PERIODS2
    '''
    r = Refresh()
    r.import_table_w_datapump(tables=tables)

if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception('Got exception on main handler')
        raise

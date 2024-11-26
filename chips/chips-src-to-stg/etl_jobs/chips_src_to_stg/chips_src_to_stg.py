# enable imports from parent directory and other specified paths
import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
from src.peoplesoft_etl import *
import logging

this_dir = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=f'{this_dir}\chips_src_to_stg.log',
    filemode='w',
    level=logging.INFO, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

def main():
    """
    Main function to initiate the ETL process for specified PeopleSoft tables.
    
    This function defines a list of endpoint-table pairs and calls the
    build_tables function to perform the ETL tasks. The function is designed
    to handle multiple tables efficiently using specified parameters for task
    workers and sleep time.
    """
    endpoint_table_pairs = [
        # Rebuild entire tables
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

    # Start the ETL process with specified parameters
    build_tables(
        endpoint_table_pairs = endpoint_table_pairs,
        n_task_workers = 10,
        start_task_sleep_time = 2,
    )

if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception('Got exception on main handler')
        raise

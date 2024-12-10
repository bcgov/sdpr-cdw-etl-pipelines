import pandas as pd
import numpy as np
import logging
import yaml
import sys
from dotenv import load_dotenv
import os 
load_dotenv()
base_dir = os.getenv('MAIN_BASE_DIR')
print(base_dir)
sys.path.append(base_dir)
from utils.oracle_db import OracleDB
from src.chefs_api import ChefsApi
import src.utils as utils

this_dir = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)
logging.basicConfig(
    # filename=f'{this_dir}\.log',
    # filemode='w',
    level=logging.DEBUG, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

# print options for DataFrames
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# get config values
with open('chefs-forms\hr-data-usage-agreement\config.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)
    api_key_secret = config['api-secret']
    form_id = config['form-id']
    form_version_id = config['form-version-id']

# initialize objects for ETL
oracle_db = OracleDB(conn_str_key_endpoint=os.getenv('ORACLE_CONN_STRING_KEY'))
chefs_api = ChefsApi(
    api_key_secret=api_key_secret, 
    form_id=form_id, 
    form_version_id=form_version_id
)


def extract():
    """
    Gets submission data and returns selected data fields in a df
    """
    # get form submissions data
    submissions = chefs_api.get_json(chefs_api.submissions_endpoint)

    # initialize the dict that will be returned as a df
    data_dict = {
        "employee": [],
        "status": [],
        "requires_fte_data": [],
        "requires_stiip_data": [],
        "requires_time_leave_data": [],
        "requires_employee_data": [],
        "requires_pay_cost_earnings_data": [],
        "requires_employee_movement_data": [],
        "requires_org_heirarchy_data": [],
        "requires_other_data": [],
        "details_of_other_required_data": [],
        "duration_of_access": [],
        "access_end_date": [],
        "rational_for_access": [],
        "submission_date": [],
        "employee_idir": [],
        "employee_position": [],
        "employee_branch": [],
        "employee_divison": [],
        "employee_email": [],
        "supervisor_name": [],
        "supervisor_position": [],
        "supervisor_email": [],
        "deleted": [],
    }

    # extract submission data and add it to data_dict
    for submission in submissions:
        deleted = submission['deleted']
        submission_id = submission['id']
        status = chefs_api.get_current_status(submission_id=submission_id)
        # keys = submission.keys()
        sub = submission.get('submission')
        # submissions_keys = s.keys()
        s = sub.get('data')
        # s_keys = s.keys()
        # print(s_keys)

        date = s.get('date')
        submit = s.get('submit')
        editGrid1 = s.get('editGrid1')
        lateEntry = s.get('lateEntry')
        simpleemail = s.get('simpleemail')
        positionTitle1 = s.get('positionTitle1')
        supervisorsName = s.get('supervisorsName')
        durationOfAccess = s.get('durationOfAccess')
        provideClearRationaleForAccess2 = s.get('provideClearRationaleForAccess2')
        enterDataRelatedToAnyOfTheFollowingDomains = s.get('enterDataRelatedToAnyOfTheFollowingDomains')
        ifOtherPleaseProvideDetailsBelow = s.get('ifOtherPleaseProvideDetailsBelow')
        ifThisIsTemporaryPleaseProvideTheAccessEndDate = s.get('ifThisIsTemporaryPleaseProvideTheAccessEndDate')
        requires_other_data = enterDataRelatedToAnyOfTheFollowingDomains.get('other')
        requires_fte_data = enterDataRelatedToAnyOfTheFollowingDomains.get('fteData')
        requires_stiip_data = enterDataRelatedToAnyOfTheFollowingDomains.get('stiipData')
        requires_time_leave_data = enterDataRelatedToAnyOfTheFollowingDomains.get('timeLeave')
        requires_employee_data = enterDataRelatedToAnyOfTheFollowingDomains.get('employeeData')
        requires_pay_cost_earnings_data = enterDataRelatedToAnyOfTheFollowingDomains.get('payCostEarnings')
        requires_employee_movement_data = enterDataRelatedToAnyOfTheFollowingDomains.get('employeeMovement')
        requires_org_heirarchy_data = enterDataRelatedToAnyOfTheFollowingDomains.get('organizationalHierarchy')

        for user in editGrid1:
            idir = user.get('idir')
            branch = user.get('branch')
            nameOfUser = user.get('nameOfUser')
            positionTitle = user.get('positionTitle')
            positionTitle2 = user.get('positionTitle2')
            simpleemailadvanced = user.get('simpleemailadvanced')

            curr_data_dict = {
                "employee": nameOfUser,
                "status": status,
                "requires_fte_data": requires_fte_data,
                "requires_stiip_data": requires_stiip_data,
                "requires_time_leave_data": requires_time_leave_data,
                "requires_employee_data": requires_employee_data,
                "requires_pay_cost_earnings_data": requires_pay_cost_earnings_data,
                "requires_employee_movement_data": requires_employee_movement_data,
                "requires_org_heirarchy_data": requires_org_heirarchy_data,
                "requires_other_data": requires_other_data,
                "details_of_other_required_data": ifOtherPleaseProvideDetailsBelow,
                "duration_of_access": durationOfAccess,
                "access_end_date": ifThisIsTemporaryPleaseProvideTheAccessEndDate,
                "rational_for_access": provideClearRationaleForAccess2,
                "submission_date": date,
                "employee_idir": idir,
                "employee_position": positionTitle,
                "employee_branch": branch,
                "employee_divison": positionTitle2,
                "employee_email": simpleemailadvanced,
                "supervisor_name": supervisorsName,
                "supervisor_position": positionTitle1,
                "supervisor_email": simpleemail,
                "deleted": deleted,
            }

            for key, value in data_dict.items():
                value.append(curr_data_dict[key])

    # turn data_dict into a df
    data_df = pd.DataFrame.from_dict(data_dict)
    return data_df


def transform(extracted_data: pd.DataFrame):
    """
    Transforms the extracted data.

    Args:
        extracted_data (pd.DataFrame): The extracted data.

    Returns:
        pd.DataFrame: The transformed data.
    """
    try:
        # get the initial data model mapping used to associate endpoint fields with target table
        # columns
        data_model_mapping = utils.data_model_mapping(
            utils.source_data_model(extracted_data),
            utils.target_data_model(
                owner='CPERCIVA', table_name='HR_DATA_USAGE_AGREEMENT_SUBMISSION', db=oracle_db
            ),
        )

        # identify discrepancies and consistencies in the data model mapping
        data_model_discrepancies = utils.data_model_discrepancies(data_model_mapping)
        data_model_consistencies = utils.data_model_consistencies(data_model_mapping)

        # transform the extracted data
        consistent_col_names_src = data_model_consistencies["col_name_src"].tolist()
        transformed_data = extracted_data[consistent_col_names_src]
        transformed_data = utils.apply_data_type_transformations(
            data_model_consistencies, transformed_data
        )

        # update the column names for the transformed data from the endpoint field names to the
        # target table names
        consistent_col_names_target = data_model_consistencies["col_name_target"].tolist()
        transformed_data.columns = consistent_col_names_target

        transformed_data = transformed_data.replace({np.nan: None})

        return transformed_data

    except AttributeError:
        logger.exception('ETLEngine.transform encountered an AttributeError and ignored it')


def load(transformed_data: pd.DataFrame, truncate_first: bool = True) -> None:
    """
    Loads data into the database.

    Args:
        transformed_data (pd.DataFrame): The transformed data to be loaded into the database.
        truncate_first (bool, optional): If True, truncates the table before loading. Defaults to True.
    """
    oracle_db.default_load(
        table_owner='CPERCIVA',
        table_name='HR_DATA_USAGE_AGREEMENT_SUBMISSION',
        cols_to_load_list=transformed_data.columns.tolist(),
        writeRows=list(transformed_data.itertuples(index=False, name=None)),
        truncate_first=truncate_first,
    )
        

def etl():
    """Runs the ETL functions."""
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data=transformed_data)


if __name__ == '__main__':
    etl()
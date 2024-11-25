from src.chefs_api import ChefsApi
from src.oracle_db import OracleDB
import src.utils as utils
import pandas as pd
import numpy as np
import logging
import yaml

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
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
oracle_db = OracleDB(conn_str_key_endpoint='CW1D_ETL')
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
        "submission_id": [],
        "confirmation_id": [],
        "submission_date": [],
        "status": [],
        "idir": [],
        "name": [],
        "branch": [],
        "division": [],
        "email": [],
        "position": [],
        "supervisors_name": [],
        "supervisors_position": [],
        "supervisors_email": [],
        "draft": [],
        "deleted": [],
        "duration_of_access": [],
        "requires_fte_data": [],
        "requires_stiip_data": [],
        "requires_time_leave_data": [],
        "requires_employee_data": [],
        "requires_pay_cost_earnings_data": [],
        "requires_employee_movement_data": [],
        "requires_org_heirarchy_data": [],
        "requires_other_data": [],
        "rationale_for_access": [],
        "details_about_required_data": [],
    }

    # extract submission data and add it to data_dict
    for s in submissions:
        submission_id = s['id']
        status = chefs_api.get_current_status(submission_id=submission_id)
        formVersionId = s['formVersionId']
        confirmation_id = s['confirmationId']
        draft = s['draft']
        deleted = s['deleted']
        submission = s['submission']
        createdBy = s['createdBy']
        createdAt = s['createdAt']
        updatedBy = s['updatedBy']
        updatedAt = s['updatedAt']

        submission_data = submission['data']
        state = submission['state']
        _vnote = submission['_vnote']
        metadata = submission['metadata']

        submission_date = submission_data['date']
        idir = submission_data['idir']
        branch = submission_data['branch']
        submit = submission_data['submit']
        division = submission_data['division']
        lateEntry = submission_data['lateEntry']
        name = submission_data['nameOfUser']
        email = submission_data['simpleemailadvanced']
        position = submission_data['positionTitle']
        supervisors_position = submission_data['positionTitle1']
        supervisors_email = submission_data['simpleemail']
        simpletextarea = submission_data['simpletextarea']
        supervisors_name = submission_data['supervisorsName']
        duration_of_access = submission_data['durationOfAccess']
        simplesignatureadvanced = submission_data['simplesignatureadvanced']
        rationale_for_access = submission_data['provideClearRationaleForAccess2']
        pleaseUploadTheUsersSignedAgreement = submission_data['pleaseUploadTheUsersSignedAgreement']
        required_data_domains = submission_data['enterDataRelatedToAnyOfTheFollowingDomains']
        details_about_required_data = submission_data['provideSufficientDetailAboutTheTablesReportsDashboardsReportGroupingsOrDataRequired2']

        requires_other_data = required_data_domains['others']
        requires_fte_data = required_data_domains['fteData']
        requires_stiip_data = required_data_domains['stiipData']
        requires_time_leave_data = required_data_domains['timeLeave']
        requires_employee_data = required_data_domains['employeeData']
        requires_pay_cost_earnings_data = required_data_domains['payCostEarnings']
        requires_employee_movement_data = required_data_domains['employeeMovement']
        requires_org_heirarchy_data = required_data_domains['organizationalHierarchy']

        curr_data_dict = {
            "submission_id": submission_id,
            "confirmation_id": confirmation_id,
            "submission_date": submission_date,
            "status": status,
            "idir": idir,
            "name": name,
            "branch": branch,
            "division": division,
            "email": email,
            "position": position,
            "supervisors_name": supervisors_name,
            "supervisors_position": supervisors_position,
            "supervisors_email": supervisors_email,
            "draft": draft,
            "deleted": deleted,
            "duration_of_access": duration_of_access,
            "requires_fte_data": requires_fte_data,
            "requires_stiip_data": requires_stiip_data,
            "requires_time_leave_data": requires_time_leave_data,
            "requires_employee_data": requires_employee_data,
            "requires_pay_cost_earnings_data": requires_pay_cost_earnings_data,
            "requires_employee_movement_data": requires_employee_movement_data,
            "requires_org_heirarchy_data": requires_org_heirarchy_data,
            "requires_other_data": requires_other_data,
            "rationale_for_access": rationale_for_access,
            "details_about_required_data": details_about_required_data,
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
                owner='ETL', table_name='HR_DATA_USAGE_AGREEMENT_SUBMISSIONS', db=oracle_db
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
        table_owner='ETL',
        table_name='HR_DATA_USAGE_AGREEMENT_SUBMISSIONS',
        insert_cols=utils.insert_cols_str(transformed_data.columns),
        number_of_cols=len(transformed_data.columns.tolist()),
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
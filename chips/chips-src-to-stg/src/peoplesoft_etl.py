from src.async_peoplesoft_api import AsyncPeopleSoftAPI
from src.peoplesoft_api import PeopleSoftAPI
from src.oracle_db import OracleDB
from src.async_worker import AsyncWorker
from src.etl_engine import ETLEngine
import asyncio
import aiohttp
import datetime as dt
import yaml
import traceback
import logging
import pandas as pd
from dotenv import load_dotenv
import os

logger = logging.getLogger('__main__.' + __name__)

load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
with open(base_dir + '\\' + 'config.yml', 'r') as file:
    conf = yaml.safe_load(file)


def set_n_records_per_task(n_fields_at_endpoint: int) -> int:
    """
    Returns the number of records to process per ETL task based on the number of fields.

    Args:
        n_fields_at_endpoint (int): The number of fields at the endpoint.

    Returns:
        int: The number of records to process per ETL task.
    """
    if n_fields_at_endpoint < 25:
        n_records_per_task = 10000
    elif n_fields_at_endpoint < 30:
        n_records_per_task = 7500
    elif n_fields_at_endpoint < 35:
        n_records_per_task = 5000
    else:
        n_records_per_task = 2500
    return n_records_per_task


def upsert_table_build_status(
    db: OracleDB,
    build_status: str,
    table_name: str,
    table_owner: str,
    build_time: dt.datetime,
    build_start: dt.datetime,
    build_end: dt.datetime,
    records_at_endpoint: int,
    fields_at_endpoint: int,
    rows_in_oracle: int,
    cols_in_oracle: int,
    rows_minus_recs: int,
    duplicates_in_oracle: int,
    last_rerun_of_failed_requests: dt.datetime,
):
    """
    Upserts records into the ETL table build status in Oracle.

    Args:
        db (OracleDB): The OracleDB instance used for database operations.
        build_status (str): The current status of the build.
        table_name (str): The name of the table being built.
        table_owner (str): The owner of the table.
        build_time (datetime): The total time taken for the build.
        build_start (datetime): The start time of the build.
        build_end (datetime): The end time of the build.
        records_at_endpoint (int): The number of records available at the endpoint.
        fields_at_endpoint (int): The number of fields at the endpoint.
        rows_in_oracle (int): The number of rows in the Oracle table.
        cols_in_oracle (int): The number of columns in the Oracle table.
        rows_minus_recs (int): The difference between rows in Oracle and records processed.
        duplicates_in_oracle (int): The count of duplicate records in Oracle.
        last_rerun_of_failed_requests (datetime): The last time failed requests were rerun.
    """
    db.upsert(
        table_owner='etl',
        table_name='table_build_status',
        on_str="""
            table_name = :this_table_name
            and table_owner = :this_table_owner
        """,
        update_set_str="""
            tgt.build_status = :build_status, 
            tgt.build_time = :build_time,
            tgt.build_start = :build_start,
            tgt.build_end = :build_end,
            tgt.records_at_endpoint = :records_at_endpoint,
            tgt.fields_at_endpoint = :fields_at_endpoint,
            tgt.rows_in_oracle = :rows_in_oracle,
            tgt.cols_in_oracle = :cols_in_oracle,
            tgt.rows_minus_recs = :rows_minus_recs,
            tgt.duplicates_in_oracle = :duplicates_in_oracle,
            tgt.last_rerun_of_failed_requests = :last_rerun_of_failed_requests
        """,
        insert_tgt_cols_str="""
            tgt.build_status, 
            tgt.table_name,
            tgt.table_owner,
            tgt.build_time,
            tgt.build_start,
            tgt.build_end,
            tgt.records_at_endpoint,
            tgt.fields_at_endpoint,
            tgt.rows_in_oracle,
            tgt.cols_in_oracle,
            tgt.rows_minus_recs,
            tgt.duplicates_in_oracle,
            tgt.last_rerun_of_failed_requests
        """,
        insert_vals_str="""
            :build_status,
            :this_table_name,
            :this_table_owner,
            :build_time,
            :build_start,
            :build_end,
            :records_at_endpoint,
            :fields_at_endpoint,
            :rows_in_oracle,
            :cols_in_oracle,
            :rows_minus_recs,
            :duplicates_in_oracle,
            :last_rerun_of_failed_requests
        """,
        parameters={
            'build_status': build_status,
            'this_table_name': table_name,
            'this_table_owner': table_owner,
            'build_time': build_time,
            'build_start': build_start,
            'build_end': build_end,
            'records_at_endpoint': records_at_endpoint,
            'fields_at_endpoint': fields_at_endpoint,
            'rows_in_oracle': rows_in_oracle,
            'cols_in_oracle': cols_in_oracle,
            'rows_minus_recs': rows_minus_recs,
            'duplicates_in_oracle': duplicates_in_oracle,
            'last_rerun_of_failed_requests': last_rerun_of_failed_requests,
        }
    )


def get_last_rerun_of_failed_requests(db: OracleDB, table: str):
    """
    Retrieves the last rerun timestamp of failed requests from the ETL status table.

    Args:
        db (OracleDB): The OracleDB instance used for database operations.
        table (str): The name of the table to query.

    Returns:
        datetime: The last rerun timestamp of failed requests.
    """
    last_rerun_dt = db.query_to_df(
        query_string="""
            select last_rerun_of_failed_requests
            from etl.table_build_status
            where table_name = :this_table
        """,
        parameters={'this_table': table}
    ).iat[0, 0]
    return last_rerun_dt


async def get_pay_dates_list(etl_engine: ETLEngine) -> list:
    """
    Gets a list of pay dates for endpoint in etl_engine.
    Requires: the endpoint has a sibling endpoint of the form [endpoint]_pay_dates.
    """
    endpoint = etl_engine.api_endpoint
    pay_dates_endpoint = endpoint[:-8] + '_pay_dates'
    async with aiohttp.ClientSession(base_url=etl_engine.api.base_url) as session:
        pay_dates = await etl_engine.api.get_items(
            session=session, endpoint=pay_dates_endpoint, params={'limit': 10000}
        )
    df = pd.DataFrame.from_dict(pay_dates)
    dates = df['pay_end_dt'].to_list()
    return dates


async def refresh_last_n_pay_end_dates(
    etl_engine: ETLEngine, last_n_pay_end_dates: int = 2
) -> None:
    """
    For endpoints that have 'payenddate' as a query parameter and 'PAY_END_DT' as the corresponding 
    column in the Oracle table, this function gets all records for the last_n_pay_end_dates from 
    endpoint and loads them into table_name in Oracle.

    Args:
        etl_engine (ETLEngine): The ETLEngine instance used for ETL operations.
        last_n_pay_end_dates (int, optional): The number of last pay end dates to process (default is 2).
    """
    endpoint = etl_engine.api_endpoint

    etl_engine.worker.task_count = 1
    etl_engine.worker.start_task_sleep_time = 0.1

    dates = await get_pay_dates_list(etl_engine=etl_engine)
    last_n_dates = dates[-last_n_pay_end_dates:]
    list_of_params = [{'payenddate': payenddate} for payenddate in last_n_dates]

    # grant delete on the current table to ETL
    chips_stg_db = OracleDB(conn_str_key_endpoint=conf['chips_stg_conn_str_subkey'])
    chips_stg_db.grant(
        grant_type='delete', 
        on_str=f'{etl_engine.oracle_table_owner}.{etl_engine.oracle_table_name}', 
        to_str='ETL'
    )
    chips_stg_db.close_cursor()

    for pay_end_date in last_n_dates:
        etl_engine.oracledb.delete(
            table_owner=etl_engine.oracle_table_owner, 
            table_name=etl_engine.oracle_table_name,
            where_str="PAY_END_DT = to_date(:payenddate, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
            parameters={'payenddate': pay_end_date}
        )

    # add the records for the pay end date to the table
    await etl_engine.insert_records_using_params(
        endpoint=endpoint, 
        list_of_params=list_of_params,
        n_records_per_task=300000,
        n_requests_per_session=500,
    )

    # re-run failed requests
    await etl_engine.rerun_etl_for_failed_requests(endpoint=endpoint)


async def refresh_entire_pay_date_table(
    etl_engine: ETLEngine
) -> None:
    """
    For endpoints that have 'payenddate' as a query parameter and 'PAY_END_DT' as the corresponding 
    column in the Oracle table, this function gets all records from the [endpoint]_pay_dates 
    endpoint and loads them into table_name in Oracle.

    Args:
        etl_engine (ETLEngine): The ETLEngine instance used for ETL operations.
    """
    endpoint = etl_engine.api_endpoint

    etl_engine.worker.task_count = 1
    etl_engine.worker.start_task_sleep_time = 0.1

    dates = await get_pay_dates_list(etl_engine=etl_engine)
    list_of_params = [{'payenddate': payenddate} for payenddate in dates]

    # grant delete on the current table to ETL
    # chips_stg_db = OracleDB(conn_str_key_endpoint=conf['chips_stg_conn_str_subkey'])
    # chips_stg_db.grant(
    #     grant_type='delete', 
    #     on_str=f'{etl_engine.oracle_table_owner}.{etl_engine.oracle_table_name}', 
    #     to_str='ETL'
    # )
    # chips_stg_db.close_cursor()

    etl_engine.oracledb.truncate(
        table_owner=etl_engine.oracle_table_owner, 
        table_name=etl_engine.oracle_table_name
    )

    # add the records for the pay end date to the table
    await etl_engine.insert_records_using_params(
        endpoint=endpoint, 
        list_of_params=list_of_params,
        n_records_per_task=300000,
        n_requests_per_session=500,
    )

    # re-run failed requests
    await etl_engine.rerun_etl_for_failed_requests(endpoint=endpoint)


async def update_pay_end_dates_in_range(
    etl_engine: ETLEngine, min_date: str, max_date: str
) -> None:
    """
    For endpoints that have 'payenddate' as a query parameter and 'PAY_END_DT' as the corresponding: 
    - Updates records for pay end dates within a specified date range.
    - Deletes existing records for these pay end dates before adding the new records.

    Args:
        etl_engine (ETLEngine): The ETLEngine instance used for ETL operations.
        min_date (str): The minimum date for the range (format: 'YYYY-MM-DD').
        max_date (str): The maximum date for the range (format: 'YYYY-MM-DD').
    """
    dt_min_date = dt.datetime.strptime(min_date, '%Y-%m-%d')
    dt_max_date = dt.datetime.strptime(max_date, '%Y-%m-%d')

    endpoint = etl_engine.api_endpoint

    etl_engine.worker.task_count = 1
    etl_engine.worker.start_task_sleep_time = 2

    # Get available pay end dates
    dates = await get_pay_dates_list(etl_engine=etl_engine)
    dt_dates = [dt.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ') for date in dates]
    dt_dates_in_range = []
    for date in dt_dates:
        if date >= dt_min_date and date <= dt_max_date:
            dt_dates_in_range.append(date)
    dates_in_range = [dt.datetime.strftime(dt_date, '%Y-%m-%dT%H:%M:%SZ') for dt_date in dt_dates_in_range]
    list_of_params = [{'payenddate': payenddate} for payenddate in dates_in_range]

    for pay_end_date in dates_in_range:
        etl_engine.oracledb.delete(
            table_owner=etl_engine.oracle_table_owner, 
            table_name=etl_engine.oracle_table_name,
            where_str="PAY_END_DT = to_date(:payenddate, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
            parameters={'payenddate': pay_end_date}
        )

    # add the records for the pay end date to the table
    await etl_engine.insert_records_using_params(
        endpoint=endpoint, 
        list_of_params=list_of_params,
        n_records_per_task=300000,
        n_requests_per_session=500,
    )

    # re-run failed requests
    await etl_engine.rerun_etl_for_failed_requests(endpoint=endpoint)


async def run_etl_worker(
    etl_engine:  ETLEngine, 
    endpoint: str, 
    n_records_per_task: int, 
    n_workers: int = 20, 
    start_task_sleep_time: float = 1,
    incremental_refresh: bool = True,
) -> None:
    """
    Runs async ETL tasks using a worker.

    Args:
        etl_engine (ETLEngine): The ETLEngine instance for ETL operations.
        endpoint (str): The API endpoint.
        n_records_per_task (int): The number of records processed per ETL task.
        n_workers (int): The number of workers running asynchronously.
        start_task_sleep_time (float): Sleep time in seconds after adding a task to relieve load on API.
    """
    etl_engine.worker = AsyncWorker(
        task_count=n_workers, start_task_sleep_time=start_task_sleep_time
    )

    if endpoint[-8:] == '_by_date':
        if incremental_refresh:
            await refresh_last_n_pay_end_dates(
                etl_engine=etl_engine, last_n_pay_end_dates=3
            )
        else:
            await refresh_entire_pay_date_table(etl_engine=etl_engine)
        # await update_pay_end_dates_in_range(
        #     etl_engine=etl_engine, min_date='2004-05-29', max_date='2011-12-17'
        # )

    elif endpoint[-10:] == '_pay_dates':
        await etl_engine.rebuild_table_using_params(
            endpoint=endpoint, list_of_params=[{}], n_records_per_task=n_records_per_task,
        )

    else:
        await etl_engine.rebuild_table_using_pagination(
            endpoint=endpoint, n_records_per_task=n_records_per_task,
        )
        await etl_engine.rerun_etl_for_failed_requests(endpoint=endpoint)


def build_tables(
    endpoint_table_pairs: list[str],
    n_task_workers: int = 25,
    start_task_sleep_time: int = 1,
    incremental_refresh: bool = True,
) -> None:
    """
    Builds tables by processing records from specified API endpoints.

    Args:
        endpoint_table_pairs (list[str]): List of tuples containing endpoint and table name.
        n_task_workers (int): Number of ETL tasks that can run concurrently.
        start_task_sleep_time (int): Sleep time after API requests to prevent server overload.
    """
    # Job metrics
    job_start = dt.datetime.now()

    # Initialize objects
    non_async_api = PeopleSoftAPI()
    etl_db = OracleDB(conn_str_key_endpoint=conf['etl_conn_str_subkey'])
    api = AsyncPeopleSoftAPI(oracledb=etl_db)

    def build_table(endpoint: str, table: str):
        """
        Builds a specific table from the provided endpoint.

        Args:
            endpoint (str): The API endpoint for the data.
            table (str): The name of the table to build.
        """
        table_owner = "CHIPS_STG"
        build_start = dt.datetime.now()

        records_at_endpoint = non_async_api.get_record_count(endpoint)
        fields_at_endpoint = len(asyncio.run(api.get_fields(endpoint)))
        cols_in_oracle = etl_db.get_col_count(table_owner=table_owner, table_name=table)

        upsert_table_build_status(
            db=etl_db,
            build_status="in-progress",
            table_name=table,
            table_owner=table_owner,
            build_time=None,
            build_start=build_start,
            build_end=None,
            records_at_endpoint=records_at_endpoint,
            fields_at_endpoint=fields_at_endpoint,
            rows_in_oracle=None,
            cols_in_oracle=cols_in_oracle,
            rows_minus_recs=None,
            duplicates_in_oracle=None,
            last_rerun_of_failed_requests=build_start,
        )

        try:
            # Create ETL object
            etl_engine = ETLEngine(
                api=api,
                api_endpoint=endpoint,
                oracledb=etl_db,
                oracle_table_owner="CHIPS_STG",
                oracle_table_name=table,
                cache={
                    'build_start': build_start,
                    'records_at_endpoint': records_at_endpoint,
                }
            )

            fields_at_endpoint = len(asyncio.run(api.get_fields(endpoint)))
            n_records_per_task = set_n_records_per_task(fields_at_endpoint)

            # Run ETL
            asyncio.run(
                run_etl_worker(
                    etl_engine=etl_engine,
                    endpoint=endpoint,
                    n_records_per_task=n_records_per_task,
                    n_workers=n_task_workers,
                    start_task_sleep_time=start_task_sleep_time,
                    incremental_refresh=incremental_refresh,
                )
            )
            logger.info(f"Successfuly built {table}")

            # Logging
            build_end = dt.datetime.now()
            records_at_endpoint = non_async_api.get_record_count(endpoint) # num recs after ETL
            rows_in_oracle = etl_db.get_row_count(table_owner=table_owner, table_name=table)
            cols_in_oracle = etl_db.get_col_count(table_owner=table_owner, table_name=table)
            rows_minus_recs = rows_in_oracle - records_at_endpoint
            duplicates_in_oracle = etl_db.count_dups_in_oracle(
                table_owner=table_owner, table_name=table
            )
            last_rerun_of_failed_requests = get_last_rerun_of_failed_requests(
                db=etl_db, table=table
            )

            upsert_table_build_status(
                db=etl_db,
                build_status="complete",
                table_name=table,
                table_owner=table_owner,
                build_time=build_end - build_start,
                build_start=build_start,
                build_end=build_end,
                records_at_endpoint=records_at_endpoint,
                fields_at_endpoint=fields_at_endpoint,
                rows_in_oracle=rows_in_oracle,
                cols_in_oracle=cols_in_oracle,
                rows_minus_recs=rows_minus_recs,
                duplicates_in_oracle=duplicates_in_oracle,
                last_rerun_of_failed_requests=last_rerun_of_failed_requests,
            )

        except Exception:
            print(traceback.format_exc())
            build_end = dt.datetime.now()
            try:
                records_at_endpoint = non_async_api.get_record_count(endpoint) # num recs after ETL
                fields_at_endpoint = len(asyncio.run(api.get_fields(endpoint)))
                rows_in_oracle = etl_db.get_row_count(table_owner=table_owner, table_name=table)
                cols_in_oracle = etl_db.get_col_count(table_owner=table_owner, table_name=table)
                duplicates_in_oracle = etl_db.count_dups_in_oracle(
                    table_owner=table_owner, table_name=table
                )
                last_rerun_of_failed_requests = get_last_rerun_of_failed_requests(
                    db=etl_db, table=table
                )
            except Exception as e:
                logging.exception(f'Got exception when attempting to build table {table}')
                records_at_endpoint = None
                fields_at_endpoint = None
                rows_in_oracle = None
                cols_in_oracle = None
                rows_minus_recs = None
                duplicates_in_oracle = None
                last_rerun_of_failed_requests = None

            upsert_table_build_status(
                db=etl_db,
                build_status="incomplete",
                table_name=table,
                table_owner=table_owner,
                build_time=build_end - build_start,
                build_start=build_start,
                build_end=build_end,
                records_at_endpoint=records_at_endpoint,
                fields_at_endpoint=fields_at_endpoint,
                rows_in_oracle=rows_in_oracle,
                cols_in_oracle=cols_in_oracle,
                rows_minus_recs=rows_minus_recs,
                duplicates_in_oracle=duplicates_in_oracle,
                last_rerun_of_failed_requests=last_rerun_of_failed_requests,
            )

            # exit(16)

    for endpoint, table in endpoint_table_pairs:
        build_table(
            endpoint=endpoint,
            table=table,
        )

    logger.info(f"HCDWLPWA ran successfully | runtime: {dt.datetime.now() - job_start}")

    # logger.info("Building source vs stage comparison tables")
    # api_catalog_schemas_etl.main()
    # api_catalog_endpoints_etl.main()

    # exit(1)


def peoplesoft_etl_engine(
    endpoint: str, table_owner: str, table_name: str, task_count: int, start_task_sleep_time: int
) -> ETLEngine:
    """
    Returns an ETLEngine suitable for ETL of PeopleSoft tables.

    Args:
        endpoint (str): The API endpoint for the data.
        table_owner (str): The owner of the target Oracle table.
        table_name (str): The name of the target Oracle table.
        task_count (int): The number of concurrent tasks.
        start_task_sleep_time (int): Sleep time after API requests.

    Returns:
        ETLEngine: Configured ETLEngine instance.
    """
    etl_db = OracleDB(conn_str_key_endpoint=conf['etl_conn_str_subkey'])
    api = AsyncPeopleSoftAPI(oracledb=etl_db)
    non_async_api = PeopleSoftAPI()
    worker = AsyncWorker(task_count=task_count, start_task_sleep_time=start_task_sleep_time)

    build_start = dt.datetime.now()
    records_at_endpoint = non_async_api.get_record_count(endpoint)

    etl_engine = ETLEngine(
        api=api,
        api_endpoint=endpoint,
        oracledb=etl_db,
        oracle_table_owner=table_owner,
        oracle_table_name=table_name,
        worker=worker,
        cache={
            'build_start': build_start,
            'records_at_endpoint': records_at_endpoint,
        }
    )

    return etl_engine


def manually_add_records_to_table(
    endpoint: str, params: dict, table_owner: str, table_name: str
):
    """
    Adds records to the specified Oracle table from the given API endpoint.

    Args:
        endpoint (str): The API endpoint for the data.
        params (dict): The parameters for the API request.
        table_owner (str): The owner of the target Oracle table.
        table_name (str): The name of the target Oracle table.
    """
    etl_engine = peoplesoft_etl_engine(
        endpoint=endpoint, table_owner=table_owner, table_name=table_name
    )
    asyncio.run(etl_engine.add_records_to_table(endpoint=endpoint, params=params))
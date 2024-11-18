import src.utils as utils
from src.oracle_db import OracleDB
from src.peoplesoft_api import PeopleSoftAPI
from src.async_peoplesoft_api import AsyncPeopleSoftAPI
from src.async_worker import AsyncWorker
from src.primary_keys import PrimaryKeys
import oracledb
import warnings
import pandas as pd
import numpy as np
import asyncio
import aiohttp
import datetime as dt
import ast
import logging
#
logger = logging.getLogger('__main__.' + __name__)

# Display entire DataFrame when printing
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

# Warning Suppressions
pd.options.mode.chained_assignment = None  # default='warn'
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


class ETLEngine:
    """
    ETL: An ETL object consumes data from an API endpoint, transforms it, and loads it into a database.

    Args:
        api (AsyncPeopleSoftAPI): The API object for data retrieval.
        api_endpoint (str): The name of the API endpoint providing the source data (e.g. 'ps_deduction_class').
        oracledb (OracleDB): The database object for Oracle interactions.
        oracle_table_owner (str): The owner of the Oracle target table (e.g. 'CHIPS_STG').
        oracle_table_name (str): The name of the Oracle target table (e.g. 'PS_DEDUCTION_CLASS').
        worker (AsyncWorker, optional): The worker for managing async tasks. Defaults to AsyncWorker().
        cache (dict, optional): A cache for storing intermediate data. Defaults to None.
    """

    def __init__(
        self,
        api: AsyncPeopleSoftAPI,
        api_endpoint: str,
        oracledb: OracleDB,
        oracle_table_owner: str,
        oracle_table_name: str,
        worker: AsyncWorker=AsyncWorker(),
        cache: dict=None,
    ) -> None:
        self.api = api
        self.api_endpoint = api_endpoint
        self.oracledb = oracledb
        self.oracle_table_owner = oracle_table_owner
        self.oracle_table_name = oracle_table_name
        self.worker = worker
        self.cache = cache

    def extract(self, limit: int = 10000, offsets: list[int] = None) -> pd.DataFrame:
        """
        Gets data from the API and returns it in a DataFrame.

        Args:
            limit (int, optional): The maximum number of records to retrieve. Defaults to 10000.
            offsets (list[int], optional): List of offsets for pagination. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the extracted data.

        Raises:
            ExtractException: If an error occurs during extraction.
        """
        try:
            src_records = asyncio.run(self.api.get_items_concurrently(self.api_endpoint))
            self.extracted_data = pd.DataFrame(src_records)
            return self.extracted_data
        except Exception as e:
            raise ExtractException(e)

    def transform(self, extracted_data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the extracted data.

        Args:
            extracted_data (pd.DataFrame): The extracted data.

        Returns:
            pd.DataFrame: The transformed data.

        Raises:
            TransformException: If an error occurs during transformation.
        
        Properties created:
            self.data_model_discrepancies: Data model discrepancies between the API data and the Oracle data.
            self.data_model_consistencies: Data model consistencies between the API data and the Oracle data.
            self.transformed_data: The transformed data for further operations.
        """
        try:
            # get the initial data model mapping used to associate endpoint fields with target table
            # columns
            self.data_model_mapping = utils.data_model_mapping(
                utils.source_data_model(extracted_data),
                utils.target_data_model(
                    self.oracle_table_owner, self.oracle_table_name, self.oracledb
                ),
            )

            # identify discrepancies and consistencies in the data model mapping
            self.data_model_discrepancies = utils.data_model_discrepancies(self.data_model_mapping)
            self.data_model_consistencies = utils.data_model_consistencies(self.data_model_mapping)

            # transform the extracted data
            consistent_col_names_src = self.data_model_consistencies["col_name_src"].tolist()
            transformed_data = extracted_data[consistent_col_names_src]
            transformed_data = utils.apply_data_type_transformations(
                self.data_model_consistencies, transformed_data
            )

            # update the column names for the transformed data from the endpoint field names to the
            # target table names
            consistent_col_names_target = self.data_model_consistencies["col_name_target"].tolist()
            transformed_data.columns = consistent_col_names_target

            transformed_data = transformed_data.replace({np.nan: None})

            self.transformed_data = transformed_data

            return transformed_data

        except AttributeError:
            logger.debug('ETLEngine.transform encountered an AttributeError and ignored it')

        except Exception as e:
            raise TransformException(e)

    def load(self, transformed_data: pd.DataFrame, truncate_first: bool = True) -> None:
        """
        Loads data into the database.

        Args:
            transformed_data (pd.DataFrame): The transformed data to be loaded into the database.
            truncate_first (bool, optional): If True, truncates the table before loading. Defaults to True.

        Raises:
            LoadException: If an error occurs during loading.
        """
        table_owner=self.oracle_table_owner
        table_name=self.oracle_table_name
        cols_to_load_list=transformed_data.columns
        writeRows=list(transformed_data.itertuples(index=False, name=None))
        
        if truncate_first:
            self.oracledb.truncate(table_owner, table_name)

        row_params = [
            dict(zip(cols_to_load_list, list(row_vals_tuple))) for row_vals_tuple in writeRows
        ]

        try:
            self.oracledb.insert_many(
                table_owner=table_owner,
                table_name=table_name,
                cols_to_insert_list=cols_to_load_list,
                parameters=row_params,
            )

        except oracledb.DatabaseError as e:
            error, = e.args
            logger.info(f'Encountered Oracle error code: {error.code}')

            if error.code == 1:
                logger.info('''
                    Encountered ORA-00001: unique constraint violated.
                    Attempting to merge the records on the primary key instead of inserting them.
                ''')
                self.merge_new_records_on_primary_key_or_all_cols(
                    table_owner=table_owner,
                    table_name=table_name,
                    cols_to_merge_list=cols_to_load_list,
                    parameters=row_params,
                )   
            else:
                raise e


    def merge_new_records_on_primary_key_or_all_cols(
        self,
        table_owner: str,
        table_name: str,
        cols_to_merge_list: list[str],
        parameters,
    ) -> None:
        """
        Merges new records on the primary key defined or all columns if the primary key is undefined.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            cols_to_merge_list (str): The columns to be merged.
            parameters: The parameters to bind to the merge statement.
        """
        primary_key = PrimaryKeys()
        pk = primary_key.get_primary_key(pk_name=f'{table_name}_PK')

        if pk == []:
            # No PK was found in the list of PK's. So, merged on all cols (i.e. distinct rows)
            pk = cols_to_merge_list.copy()

        self.oracledb.merge_new_records(
            table_owner=table_owner, 
            table_name=table_name, 
            cols_to_merge_on_list=pk,
            cols_to_merge_list=cols_to_merge_list,
            parameters=parameters,
        )


    def get_all_failed_request_params(self, db: OracleDB, endpoint: str, table: str) -> None:
        """
        Gets failed requests for the last run of a table build from the Oracle request log table
        and retries the full ETL cycle for those requests.

        Args:
            db (OracleDB): The database object for Oracle interactions.
            endpoint (str): The API endpoint to check.
            table (str): The name of the table.

        Returns:
            list: A list of failed request parameters.
        """
        # Get the time of the last attempt to re-send failed requests for the current table build
        last_rerun_of_failed_requests = db.query_to_df(
            query_string="""
                select last_rerun_of_failed_requests
                from etl.table_build_status
                where table_name = :this_table
            """,
            parameters={'this_table': table}
        ).iat[0, 0]
        last_rerun_of_failed_requests = str(last_rerun_of_failed_requests)

        # Get a list of the failed request parameters for the current table build
        all_failed_request_params = db.query_to_df(
            query_string="""
                select request_params
                from etl.log_api_transaction_view
                where request_timestamp >= to_date(:last_rerun_of_failed_requests, 'yyyy-mm-dd hh24:mi:ss')
                    and response_status != 200
                    and endpoint = :endpoint
            """,
            parameters={'last_rerun_of_failed_requests': last_rerun_of_failed_requests, 'endpoint': endpoint}
        )['REQUEST_PARAMS'].tolist()
        all_failed_request_params = [
            ast.literal_eval(params) for params in all_failed_request_params
        ]

        # Return the list of failed request parameters for the current table build
        return all_failed_request_params


    async def etl_task(
        self, session: aiohttp.ClientSession, endpoint: str, params: dict
    ) -> None:
        """
        Makes a single GET request to the endpoint and returns only the 'items' in the response,
        then transforms and loads the data into Oracle.

        Args:
            session (aiohttp.ClientSession): The aiohttp session.
            endpoint (str): The API endpoint.
            params (dict): The request parameters.

        Returns:
            None
        """
        logger.info(f"started {endpoint} {params} | tasks running: {self.worker.running_task_count}")

        items = await self.api.get_items(session, endpoint, params)
        extracted_data = pd.DataFrame(items)
        transformed_data = self.transform(extracted_data=extracted_data)
        self.load(transformed_data=transformed_data, truncate_first=False)

        get_rows_in_oracle = self.oracledb.get_row_count(
            table_owner=self.oracle_table_owner, table_name=self.oracle_table_name
        )
        
        self.oracledb.update(
            table_owner="ETL",
            table_name="TABLE_BUILD_STATUS",
            set_str="""
                build_time = :build_time, 
                rows_in_oracle = :rows_in_oracle, 
                rows_minus_recs = :rows_minus_recs
            """,
            where_str="""
                table_name = :table_name_in_update_row
                and table_owner = :table_owner_in_update_row
            """,
            parameters={
                "build_time": dt.datetime.now() - self.cache['build_start'],
                "rows_in_oracle": get_rows_in_oracle,
                "rows_minus_recs": get_rows_in_oracle - self.cache['records_at_endpoint'],
                "table_name_in_update_row": self.oracle_table_name,
                "table_owner_in_update_row": self.oracle_table_owner,
            }
        )

        logger.info(f"ended {endpoint} {params} | tasks running: {self.worker.running_task_count}")


    async def add_records_to_table(self, endpoint: str, params: dict) -> None:
        """
        Gets items from the endpoint for the specified params and runs etl_task to load them into
        the associated Oracle table.

        Args:
            endpoint (str): The API endpoint.
            params (dict): The request parameters.

        Returns:
            None
        """
        async with aiohttp.ClientSession(base_url=self.api.base_url) as session:
            logger.info('opened new session')
            await self.etl_task(
                session=session,
                endpoint=endpoint,
                params=params,
            )
        # Zero-sleep to allow underlying connections to close
        await asyncio.sleep(0)


    async def rebuild_table_using_pagination(
        self, 
        endpoint: str, 
        n_records_per_task: int,
        n_requests_per_session: int=500,
    ) -> None:
        """
        Runs all ETL iterations to load all the data from an endpoint to a table.
        Opens a new session every n_requests_per_session and creates pages based on the total
        number of records at the endpoint at the time of execution and the record limit per request.

        Args:
            endpoint (str): The API endpoint.
            n_records_per_task (int): Number of records to fetch per task.
            n_requests_per_session (int, optional): Number of requests per session. Defaults to 500.

        Returns:
            None
        """
        self.oracledb.truncate(
            table_owner=self.oracle_table_owner, table_name=self.oracle_table_name
        )

        async with aiohttp.ClientSession(base_url=self.api.base_url) as session:
            offsets = await self.api.get_offsets(session, endpoint, n_records_per_task)

        # Open a new session after every n_requests_per_session so session doesn't timeout.
        offset_chunks = utils.chunks(offsets, n_requests_per_session)
        for offset_chunk in offset_chunks:
            async with aiohttp.ClientSession(base_url=self.api.base_url) as session:
                logger.info('opened new session')
                for offset in offset_chunk:
                    params = {"limit": n_records_per_task, "offset": offset}
                    await self.worker.add_task(
                        self.etl_task(session=session, endpoint=endpoint, params=params),
                        task_name=f"task: {endpoint} {params}"
                    )
                # keep the program alive until all the tasks are done
                while self.worker.running_task_count > 0:
                    await asyncio.sleep(0.1)
            # Zero-sleep to allow underlying connections to close
            await asyncio.sleep(0)


    async def rebuild_table_using_params(
        self, 
        endpoint: str, 
        list_of_params: list[dict],
        n_records_per_task: int,
        n_requests_per_session: int=500,
    ) -> None:
        """
        Runs all ETL iterations to load all the data from an endpoint to a table.
        Opens a new session every n_requests_per_session and processes each set of params.

        Args:
            endpoint (str): The API endpoint.
            list_of_params (list[dict]): List of parameters for each request.
            n_records_per_task (int): Number of records to fetch per task.
            n_requests_per_session (int, optional): Number of requests per session. Defaults to 500.

        Returns:
            None
        """
        self.oracledb.truncate(
            table_owner=self.oracle_table_owner, table_name=self.oracle_table_name
        )

        # Open a new session after every n_requests_per_session so session doesn't timeout.
        params_chunks = utils.chunks(list_of_params, n_requests_per_session)
        for params_chunk in params_chunks:
            async with aiohttp.ClientSession(base_url=self.api.base_url) as session:
                logger.info('opened new session')
                for params in params_chunk:
                    await self.worker.add_task(
                        self.etl_task(session=session, endpoint=endpoint, params=params),
                        task_name=f"task: {endpoint} {params}"
                    )
                # keep the program alive until all the tasks are done
                while self.worker.running_task_count > 0:
                    await asyncio.sleep(0.1)
            # Zero-sleep to allow underlying connections to close
            await asyncio.sleep(0)


    async def insert_records_using_params(
        self, 
        endpoint: str, 
        list_of_params: list[dict],
        n_records_per_task: int,
        n_requests_per_session: int=500,
    ) -> None:
        """
        Requests records for each param in list_of_params and loads the records into the associated
        table in Oracle.

        Args:
            endpoint (str): The API endpoint.
            list_of_params (list[dict]): List of parameters for each request.
            n_records_per_task (int): Number of records to fetch per task.
            n_requests_per_session (int, optional): Number of requests per session. Defaults to 500.

        Returns:
            None
        """
        # Open a new session after every n_requests_per_session so session doesn't timeout.
        params_chunks = utils.chunks(list_of_params, n_requests_per_session)
        for params_chunk in params_chunks:
            async with aiohttp.ClientSession(base_url=self.api.base_url) as session:
                logger.info('opened new session')
                for params in params_chunk:
                    await self.worker.add_task(
                        self.etl_task(session=session, endpoint=endpoint, params=params),
                        task_name=f"task: {endpoint} {params}"
                    )
                # keep the program alive until all the tasks are done
                while self.worker.running_task_count > 0:
                    await asyncio.sleep(0.1)
            # Zero-sleep to allow underlying connections to close
            await asyncio.sleep(0)


    async def rerun_etl_for_failed_requests(self, endpoint: str) -> None:
        """
        After at least one run of the ETL is complete, get the requests that failed and re-run them.
        This function iteratively attempts to process any remaining failed requests.

        Args:
            endpoint (str): The API endpoint.

        Returns:
            None
        """
        async with aiohttp.ClientSession(base_url=self.api.base_url) as session:
            logger.info('opened new session')
            logger.info('retrying any requests that failed on initial table build')
            all_params = self.get_all_failed_request_params(
                db=self.oracledb, endpoint=self.api_endpoint, table=self.oracle_table_name
            )
            # update last_rerun_of_failed_requests time before rerunning requests so we can
            # reiterate if necessary
            self.oracledb.update(
                table_owner="ETL",
                table_name="TABLE_BUILD_STATUS",
                set_str="""
                    last_rerun_of_failed_requests = :last_rerun_of_failed_requests
                """,
                where_str="""
                    table_name = :table_name_in_update_row
                    and table_owner = :table_owner_in_update_row
                """,
                parameters={
                    "last_rerun_of_failed_requests": dt.datetime.now(),
                    "table_name_in_update_row": self.oracle_table_name,
                    "table_owner_in_update_row": self.oracle_table_owner,
                }
            )
            for params in all_params:
                await self.worker.add_task(
                    self.etl_task(session=session, endpoint=endpoint, params=params),
                    task_name=f"task: {endpoint} params={params}"
                )
            # keep the program alive until all the tasks are done
            while self.worker.running_task_count > 0:
                await asyncio.sleep(0.1)
        # Zero-sleep to allow underlying connections to close
        await asyncio.sleep(0)

        # Re-iterate if any requests failed in the last run
        all_params = self.get_all_failed_request_params(
            db=self.oracledb, endpoint=self.api_endpoint, table=self.oracle_table_name
        )
        if len(all_params) > 0:
            await self.rerun_etl_for_failed_requests(endpoint=endpoint)


class ExtractException(Exception):
    """Exception raised during extraction process."""
    def __init__(self, msg="An exception occurred during extraction", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class TransformException(Exception):
    """Exception raised during transformation process."""
    def __init__(self, msg="An exception occurred during transformation", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class LoadException(Exception):
    """Exception raised during loading process."""
    def __init__(self, msg="An exception occurred during load", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class AsyncETLException(Exception):
    """Exception raised during an async ETL task."""
    def __init__(self, msg="An exception occurred during an async ETL task", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)

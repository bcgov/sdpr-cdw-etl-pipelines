import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
import src.utils as utils
from src.oracle_db import OracleDB
import aiohttp
import asyncio
import time
import jwt
import datetime as dt
from aiohttp_retry import RetryClient, FibonacciRetry
from types import SimpleNamespace
import logging
import yaml

logger = logging.getLogger('__main__.' + __name__)

with open(base_dir + '\\' + 'config.yml', 'r') as file:
    conf = yaml.safe_load(file)

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class AsyncPeopleSoftAPI:
    """
    Asynchronous client for interacting with the PeopleSoft API.

    Attributes:
        base_url (str): Base URL for the API.
        oracledb (OracleDB, optional): Database connection for logging.
    """
    
    def __init__(self, oracledb: OracleDB=None) -> None:
        self.base_url = "https://rp-api.tssi.ca"
        self.oracledb = oracledb

    def generate_auth_token(self):
        """
        Generates the authorization token for the API.

        This function reads a PEM file to create a JWT token, which is used for API authentication.

        Returns:
            str: An authorization token for API requests in the format "Bearer <token>".
        """
        pem_file_path = conf['project_sys_path'] + "\SDPR_keypair.pem"
        with open(pem_file_path, "r") as file:
            pem_key = file.read()
        epoch_time_now = int(time.time())
        epoch_expiration_time = epoch_time_now + int(60 * 60 * 24)
        jwt_payload = {
            "iss": "https://identity.oraclecloud.com/",
            "aud": "https://rp-api.tssi.ca/",
            "iat": epoch_time_now,
            "exp": epoch_expiration_time,
        }
        jwt_headers = {"kid": "BcMHRGRP", "kty": "RSA", "alg": "RS256"}
        encoded_jwt = jwt.encode(payload=jwt_payload, key=pem_key, headers=jwt_headers)
        auth_token = "Bearer " + encoded_jwt
        return auth_token

    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = {}):
        """
        Makes a GET request to the specified API endpoint.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            endpoint (str): The API endpoint to request data from.
            params (dict, optional): Query parameters to include in the request. Defaults to an empty dictionary.

        Returns:
            aiohttp.ClientResponse: The response from the API request.
        """
        url = f"/mhrgrp/{endpoint}"

        headers = {
            "Content-Type": "application/json",
            "Authorization": self.generate_auth_token(),
            "Host": "rp-api.tssi.ca",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        proxy = "http://142.34.229.249:8080"

        statuses = {x for x in range(100, 600)}
        statuses.remove(200)
        statuses.remove(403)
        statuses.remove(404)

        async def on_request_start(
            session: aiohttp.ClientSession,
            trace_config_ctx: SimpleNamespace,
            params: aiohttp.TraceRequestStartParams,
        ) -> None:
            current_attempt = trace_config_ctx.trace_request_ctx['current_attempt']
            if current_attempt > 1:
                logger.info(f'Attempt {current_attempt} for request: {params.url}')
            if current_attempt == retry_options.attempts:
                logger.info(f'Final attempt for request: {params.url}')

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.freeze()
        session.trace_configs.append(trace_config)

        retry_options = FibonacciRetry(
            attempts=30,  # How many times we should retry
            multiplier=1,  # multipler * (curr val + prev val)
            max_timeout=60.0,  # Max possible timeout between tries
            statuses=statuses,  # On which statuses we should retry
            exceptions={aiohttp.ClientError},  # On which exceptions we should retry
            retry_all_server_errors=True,
        )
        retry_client = RetryClient(
            client_session=session, 
            retry_options=retry_options,
            raise_for_status=False,
            trace_configs=[trace_config],
        )

        # Cache request data
        request_url = self.base_url + url
        request_params = params
        request_headers = headers
        request_proxy = proxy
        request_time = dt.datetime.now()

        resp = await retry_client.get(
            url=url,
            params=params,
            headers=headers,
            proxy=proxy,
            ssl=False,
            raise_for_status=False,
        )

        response_time = dt.datetime.now()
        logger.info(f"Got: {endpoint} {params} | status: {resp.status}")

        # Cache response data
        response_status = resp.status
        data = await resp.json(content_type=None)
        try:
            response_hasMore = data['hasMore']
            response_limit = data['limit']
            response_offset = data['offset']
            response_count = data['count']
            response_links = data['links']
        except KeyError:
            response_hasMore = None
            response_limit = None
            response_offset = None
            response_count = None
            response_links = None

        if self.oracledb is not None:
            self.oracledb.insert(
                table_owner='ETL',
                table_name='LOG_API_TRANSACTION',
                insert_tgt_cols_str="""
                    request_url,
                    request_params,
                    request_headers,
                    request_proxy,
                    request_timestamp,
                    request_aiohttp_session,
                    response_timestamp,
                    response_status,
                    response_has_more,
                    response_limit,
                    response_offset,
                    response_count,
                    response_links
                """,
                insert_vals_str="""
                    :request_url,
                    :request_params,
                    :request_headers,
                    :request_proxy,
                    :request_timestamp,
                    :request_aiohttp_session,
                    :response_timestamp,
                    :response_status,
                    :response_has_more,
                    :response_limit,
                    :response_offset,
                    :response_count,
                    :response_links
                """,
                parameters={
                    'request_url': request_url,
                    'request_params': str(request_params),
                    'request_headers': str(request_headers),
                    'request_proxy': request_proxy,
                    'request_timestamp': request_time,
                    'request_aiohttp_session': str(session),
                    'response_timestamp': response_time,
                    'response_status': response_status,
                    'response_has_more': response_hasMore,
                    'response_limit': response_limit,
                    'response_offset': response_offset,
                    'response_count': response_count,
                    'response_links': str(response_links),
                },
            )

        return resp

    async def get_json(
        self, session: aiohttp.ClientSession, endpoint: str, params: dict = {}
    ) -> dict:
        """
        Makes a GET request to the API and returns the response as JSON.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            endpoint (str): The API endpoint to request data from.
            params (dict, optional): Query parameters to include in the request. Defaults to an empty dictionary.

        Returns:
            dict: The response data parsed as JSON.
        """
        response = await self.get(session, endpoint, params)
        data = await response.json(content_type=None)
        return data

    async def get_items(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: dict={},
    ) -> dict:
        """
        Retrieves only the 'items' from the JSON response of a GET request.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            endpoint (str): The API endpoint to request data from.
            params (dict, optional): Query parameters to include in the request. Defaults to an empty dictionary.

        Returns:
            dict: The value for 'items' in the JSON response.
        """
        data = await self.get_json(session, endpoint, params)
        items = data["items"]
        return items

    async def get_items_tasks(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        limit: int,
        offsets: list[int] = None,
    ):
        """
        Produces tasks that request records from the specified endpoint.

        Args:
            session (aiohttp.ClientSession): The session to use for the requests.
            endpoint (str): The API endpoint to request data from.
            limit (int): The maximum number of records to retrieve per request.
            offsets (list[int], optional): Specific offsets to use for the requests. If None, tasks will cover all available data.

        Returns:
            list: A list of asyncio tasks that will retrieve records from the endpoint.
        """
        if offsets is None:
            offsets = await self.get_offsets(session, endpoint, limit)
        tasks = []
        for offset in offsets:
            tasks.append(
                asyncio.create_task(
                    self.get_items(
                        session=session,
                        endpoint=endpoint,
                        params={"limit": limit, "offset": offset},
                    )
                )
            )
            # add tasks >= 1s (=Retry-after header) apart to avoid 429 error
            await asyncio.sleep(1.1)
        return tasks

    async def get_items_concurrently(
        self,
        endpoint: str,
        limit: int = 10000,
        offset_chunk: list[int] = None,
    ) -> list:
        """
        Retrieves all items from the endpoint or only items for specified offsets.

        Args:
            endpoint (str): The API endpoint to request data from.
            limit (int, optional): The maximum number of records to retrieve per request. Defaults to 10000.
            offset_chunk (list[int], optional): Specific offsets for the requests. If None, all records are retrieved.

        Returns:
            list: A list of records retrieved from the endpoint.
        """
        connector = aiohttp.TCPConnector(force_close=True, limit=50)
        async with aiohttp.ClientSession(base_url=self.base_url, connector=connector) as session:
            tasks = await self.get_items_tasks(
                session=session, endpoint=endpoint, limit=limit, offsets=offset_chunk
            )
        # Zero-sleep to allow underlying connections to close
        await asyncio.sleep(0)
        gathered_records = await asyncio.gather(*tasks, return_exceptions=True)
        all_records = utils.flatten(gathered_records)
        return all_records

    async def get_record_count(self, session: aiohttp.ClientSession, endpoint: str) -> int:
        """
        Retrieves the total number of records available at the specified endpoint.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            endpoint (str): The endpoint that exposes the database table data.

        Returns:
            int: The total number of records available at the endpoint.
        """
        if endpoint == 'ps_pay_oth_earns_by_date':
            pay_dates_recs = await self.get_items(
                session=session, endpoint='ps_pay_oth_earns_pay_dates'
            )
            self.record_count = sum([rec['total_records'] for rec in pay_dates_recs])

        elif endpoint == 'ps_pay_oth_earns_pay_dates':
            response = await self.get_json(
                session=session, endpoint='ps_pay_oth_earns_pay_dates'
            )
            self.record_count = response['count']

        else:
            url_endpoint = "record_counts/" + endpoint
            data = await self.get_json(session=session, endpoint=url_endpoint)
            self.record_count = data["total_records"]
            
        logger.info(f"records @ {endpoint}: {self.record_count}")
        return self.record_count

    async def get_offsets(
        self, session: aiohttp.ClientSession, endpoint: str, page_size: int
    ) -> list[int]:
        """
        Generates a list of offsets for paginated requests to the specified endpoint.

        Args:
            session (aiohttp.ClientSession): The session to use for the request.
            endpoint (str): The API endpoint to request data from.
            page_size (int): The number of records to retrieve per request.

        Returns:
            list[int]: A list of offsets for use in paginated requests.
        """
        record_count = await self.get_record_count(session, endpoint)
        offsets = list(range(0, record_count, page_size))
        return offsets

    async def get_fields(self, endpoint: str) -> int:
        """
        Retrieves the field names of the records available at the specified endpoint.

        Args:
            endpoint (str): The endpoint.

        Returns:
            int: The field names of the records available at the endpoint.
        """
        async with aiohttp.ClientSession(base_url=self.base_url) as session:
            params = {"limit": 1}
            if endpoint[-8:] == '_by_date':
                endpoint = endpoint[:-8]
            data = await self.get_json(session=session, endpoint=endpoint, params=params)
        # Zero-sleep to allow underlying connections to close
        await asyncio.sleep(0)
        try:
            fields = data["items"][0].keys()
        except IndexError:
            fields = {}.keys()
        return fields

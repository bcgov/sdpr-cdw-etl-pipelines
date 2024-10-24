import requests
import time
import jwt
import urllib3
import pandas as pd
import logging
import yaml
from dotenv import load_dotenv
import os

logger = logging.getLogger('__main__.' + __name__)

load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
with open(base_dir + '\\' + 'config.yml', 'r') as file:
    conf = yaml.safe_load(file)

# Warning suppressions
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class PeopleSoftAPI:
    """
    A class to interact with the PeopleSoft API.

    This class provides methods to perform GET requests to the PeopleSoft API,
    retrieve records, and manage API catalogs.

    Attributes:
        base_url (str): The base URL for the PeopleSoft API.
    """
    def __init__(self):
        """
        Initializes the PeopleSoftAPI instance with the base URL.
        """
        self.base_url = "https://rp-api.tssi.ca/mhrgrp/"

    def get(self, url: str, params: dict = {}):
        """
        Sends a GET request to the specified URL with parameters.

        Args:
            url (str): The URL to send the request to.
            params (dict, optional): The request parameters.

        Returns:
            requests.Response: The response from the API.

        Raises:
            Exception: If an error occurs during the request.
        """
        proxy = "http://142.34.229.249:8080"
        proxies = {"http": proxy, "https": proxy}

        def generate_auth_token():
            """
            Generates the authorization token for the API
            Args:
                None
            Returns:
                An authorization token for API requests
            """
            pem_file_path = conf['project_sys_path'] + "\SDPR_keypair.pem"
            with open(pem_file_path, "r") as file:
                pem_key = file.read()
            epoch_time_now = int(time.time())
            epoch_expiration_time = epoch_time_now + int(60 * 60 * 6)
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

        headers = {
            "Content-Type": "application/json",
            "Authorization": generate_auth_token(),
            "Host": "rp-api.tssi.ca",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies=proxies,
            verify=False,
            timeout=1800,
        )

        logger.info(f"Got: {url} {params} | status: {response.status_code}")

        return response

    def get_json(self, endpoint: str, params: dict = {}) -> dict:
        """
        Sends a GET request to the specified endpoint and returns the response as JSON.

        Args:
            endpoint (str): The API endpoint to query.
            params (dict, optional): The request parameters.

        Returns:
            dict: The JSON response from the API.
        """
        url = self.base_url + endpoint
        response = self.get(url=url, params=params)
        return response.json()

    def get_record_count(self, endpoint: str) -> int:
        """
        Retrieves the number of records available at the specified endpoint.

        Args:
            endpoint (str): The endpoint exposing database table data.

        Returns:
            int: The number of records available at the endpoint.
        """
        if endpoint[-10:] == '_pay_dates':
            record_count = self.get_json(
                endpoint=endpoint
            )['count']
        else:
            if endpoint[-8:] == '_by_date':
                endpoint = endpoint[:-8]
            url = self.base_url + "record_counts/" + endpoint
            response = self.get(url)
            status = response.status_code
            if status == 200:
                data = response.json()
                record_count = data["total_records"]
            else:
                logger.info(f'get_record_count({endpoint}) did not return a 200 response')
                return 0

        return record_count

    def get_items(self, endpoint: str, params: dict=None) -> list[dict]:
        """
        Retrieves a list of items from the specified endpoint.

        Args:
            endpoint (str): The API endpoint to query.
            params (dict, optional): The request parameters.

        Returns:
            list[dict]: A list of items from the response.
        """
        url = self.base_url + endpoint
        response = self.get(url, params)
        data = response.json()
        items = data["items"]
        return items

    def get_catalog(self, endpoint: str = "open-api-catalog") -> None:
        """
        Retrieves the API catalog and initializes several attributes based on the response.

        Attributes initialized:
            self.catalog_json: The full catalog response.
            self.data_types: The Oracle to JSON data type translations.
            self.endpoints: All endpoints.
            self.schemas: Schemas for all endpoints.
        
        Args:
            endpoint (str, optional): The API endpoint for the catalog.
        """
        url = self.base_url + endpoint
        response = self.get(url, params={})
        data = response.json()
        self.catalog_json = data

        # Create Oracle data type to API data type translation DataFrame
        oracle_dtype_translation = data["components"]["schemas"]
        records = []
        for key, val in oracle_dtype_translation.items():
            record = [key, "", "", ""]
            for key2, val2 in val.items():
                if key2 == "type":
                    record[1] = val2
                elif key2 == "format":
                    record[2] = val2
                elif key2 == "pattern":
                    record[3] = val2
                else:
                    raise Exception("Open API Catalog components schema has changed")
            records += [record]
        oracle_dtype_translation_df = pd.DataFrame(records)
        oracle_dtype_translation_df.columns = [
            "oracle_data_type",
            "json_data_type",
            "json_format",
            "json_pattern",
        ]
        self.data_types = oracle_dtype_translation_df

        endpoints = []
        for k, v in data["paths"].items():
            endpoints += [k]
        self.endpoints = endpoints

        schema_data = {"endpoint": [], "field": [], "oracle_data_type": []}
        for endpoint in endpoints:
            endpoint_data = self.catalog_json["paths"][endpoint]["get"]
            description = endpoint_data["description"]
            if description == "Retrieve records from MHRGRP":
                responses = endpoint_data["responses"]["200"]
                schema = responses["content"]["application/json"]["schema"]
                properties = schema["properties"]["items"]["items"]["properties"]
                for k, v in properties.items():
                    field = k
                    oracle_data_type = v["$ref"].rsplit("/", 1)[-1]
                    schema_data["endpoint"].append(endpoint[1:])
                    schema_data["field"].append(field)
                    schema_data["oracle_data_type"].append(oracle_data_type)
        schema_df = pd.DataFrame(schema_data)
        schema_df = schema_df.merge(self.data_types, on="oracle_data_type", how="left")
        self.schemas = schema_df[
            [
                "endpoint",
                "field",
                "json_data_type",
                "json_format",
                "json_pattern",
                "oracle_data_type",
            ]
        ]

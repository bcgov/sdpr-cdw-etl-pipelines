import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)

from src.peoplesoft_api import PeopleSoftAPI

api = PeopleSoftAPI()
catalog = api.get_catalog()
endpoints = api.endpoints
for endpoint in endpoints:
    print(endpoint)

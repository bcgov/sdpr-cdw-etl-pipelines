import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
from src.peoplesoft_api import PeopleSoftAPI

api = PeopleSoftAPI()
url = api.base_url + 'ps_job'
response = api.get(url=url, params={'offset':100, 'limit': 1})
print(response)
data = response.json()
print(data.keys())
print(data)
items = data['items']
print(items[0])

import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)

from src.peoplesoft_api import PeopleSoftAPI

api = PeopleSoftAPI()
url = api.base_url + 'ps_pay_earnings'
response = api.get(url=url, params={'offset':3937500, 'limit': 9000})
print(response)
data = response.json()
print(data['count'])
print(data.keys())
items = data['items']
print(items[0])

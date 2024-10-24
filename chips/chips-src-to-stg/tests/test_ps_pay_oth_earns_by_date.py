import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)

from src.peoplesoft_api import PeopleSoftAPI

api = PeopleSoftAPI()
url = api.base_url + 'ps_pay_oth_earns_by_date'
response = api.get(url=url, params={'payenddate': '2024-09-07T00:00:00Z'})
print('response:', response)

data = response.json()
print()
print('keys:', data.keys())
print()
print('items:', data['items'][0])
print()
print('count items:', len(data['items']))
print()
print('hasMore:', data['hasMore'])
print()
print('limit:', data['limit'])
print()
print('offset:', data['offset'])
print()
print('count:', data['count'])
print()
print('links:', data['links'])

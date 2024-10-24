import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
import pandas as pd
from src.peoplesoft_api import PeopleSoftAPI

api = PeopleSoftAPI()
url = api.base_url + 'ps_pay_oth_earns_pay_dates'
response = api.get(url=url, params={'limit': 10000})
print('response:', response)
print()

data = response.json()
df = pd.DataFrame.from_dict(data['items'])
print(df)
print(df['pay_end_dt'].max())
print()
print(df['total_records'].max())
print()
print(df[df['total_records'] == df['total_records'].max()])
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

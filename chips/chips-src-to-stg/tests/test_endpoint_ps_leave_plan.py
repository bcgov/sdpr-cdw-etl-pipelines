import pandas as pd
import logging
import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)

from src.peoplesoft_api import PeopleSoftAPI

logger = logging.getLogger('__main__.' + __name__)
logging.basicConfig(
    level=logging.INFO, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

api = PeopleSoftAPI()
url = api.base_url + 'ps_leave_plan_tbl'
response = api.get(url=url, params={'offset':0, 'limit': 100})
print(response)

data = response.json()
print(data.keys())

items = data['items']
print(items[0])

df = pd.DataFrame(items)
print(df[['plan_type', 'benefit_plan']].head(10))


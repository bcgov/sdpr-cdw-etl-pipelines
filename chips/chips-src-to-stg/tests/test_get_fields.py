import aiohttp
import asyncio
import logging
import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
from src.async_peoplesoft_api import AsyncPeopleSoftAPI

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

api = AsyncPeopleSoftAPI()

for endpoint in [
    # 'ps_job',
    'ps_pay_oth_earns_by_date',
    # 'ps_pay_oth_earns_pay_dates',
]:
    fields = asyncio.run(api.get_fields(endpoint))
    print()
    print(fields)
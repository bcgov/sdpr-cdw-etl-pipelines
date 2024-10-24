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
from src.peoplesoft_api import PeopleSoftAPI

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="{levelname} ({asctime}): {message}", 
    datefmt='%d/%m/%Y %H:%M:%S',
    style='{'
)

api = AsyncPeopleSoftAPI()
non_async_api = PeopleSoftAPI()


async def async_get_record_count_ps_job():
    async with aiohttp.ClientSession(base_url=api.base_url) as session:
        rc = await api.get_record_count(session=session, endpoint='ps_job')
        return rc

print(asyncio.run(async_get_record_count_ps_job()))


def get_record_count_ps_pay_oth_earns_by_date():
    rc = non_async_api.get_record_count(endpoint='ps_pay_oth_earns_by_date')
    return rc

print(get_record_count_ps_pay_oth_earns_by_date())


async def async_get_record_count_ps_pay_oth_earns_by_date():
    async with aiohttp.ClientSession(base_url=api.base_url) as session:
        rc = await api.get_record_count(session=session, endpoint='ps_pay_oth_earns_by_date')
        return rc

print(asyncio.run(async_get_record_count_ps_pay_oth_earns_by_date()))


def get_record_count_ps_pay_oth_earns_pay_dates():
    rc = non_async_api.get_record_count(endpoint='ps_pay_oth_earns_pay_dates')
    return rc

print(get_record_count_ps_pay_oth_earns_pay_dates())


async def async_get_record_count_ps_pay_oth_earns_pay_dates():
    async with aiohttp.ClientSession(base_url=api.base_url) as session:
        rc = await api.get_record_count(session=session, endpoint='ps_pay_oth_earns_pay_dates')
        return rc

print(asyncio.run(async_get_record_count_ps_pay_oth_earns_pay_dates()))
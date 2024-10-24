import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)

import unittest
from src.async_peoplesoft_api import AsyncPeopleSoftAPI

class TestAsyncPeopleSoftAPI(unittest.TestCase):

    def setUp(self):
        self.api = AsyncPeopleSoftAPI()


if __name__ == '__main__':
    unittest.main()
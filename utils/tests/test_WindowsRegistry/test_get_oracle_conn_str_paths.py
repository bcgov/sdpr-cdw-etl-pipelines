import unittest
import sys
from dotenv import load_dotenv
import os 
load_dotenv()
base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(base_dir)
from utils.windows_registry import WindowsRegistry

class TestWindowsRegistry(unittest.TestCase):

    def setUp(self):
        self.wr = WindowsRegistry()

    def test_get_oracle_conn_str_paths(self):
        paths = self.wr.get_oracle_conn_str_paths()
        self.assertIsInstance(paths, dict)

    def test_get_oracle_conn_str(self):
        conn_str = self.wr.get_oracle_conn_str('CW1P_ETL')
        self.assertIsInstance(conn_str, str)
        print(conn_str)

if __name__ == '__main__':
    unittest.main()

from utils.oracle_db import OracleDB
import logging
import pandas as pd

logger = logging.getLogger('__main__.' + __name__)


class DataExtractor:

    def __init__(self, oracle_db: OracleDB):
        self.db = oracle_db

    def get_file_content_as_str(self, filepath):
        """Returns the contents of a file as a string"""
        opened_file = open(filepath, 'r')
        file = opened_file.read()
        opened_file.close()
        return file

    def split_sql_statements_in_str(self, sql_str):
        """Returns a list of SQL statements inside a singular string of SQL statements seperated by semi-colons"""
        sql_splitlines = sql_str.splitlines()
        sql_joined_lines = "".join(sql_splitlines)
        sql_statements = [statement for statement in sql_joined_lines.split(';') if statement]
        return sql_statements

    def sql_to_xlsx(self, sql_filepath, xlsx_filepath):
        """
        Runs SQL queries in an .sql file against an Oracle DB and returns the output of each query in 
        an .xlsx file.

        Args:
            sql_filepath (str): the path to the .sql input file 
            xlsx_filepath (str): the path to the xlsx output file
        """
        sql_str = self.get_file_content_as_str(filepath=sql_filepath)
        sql_queries = self.split_sql_statements_in_str(sql_str=sql_str)
        with pd.ExcelWriter(path=xlsx_filepath, engine='xlsxwriter') as writer:
            query_num = 1
            for query in sql_queries:
                df = self.db.query_to_df(query)
                sheet_name = f'Query {query_num}'
                df.to_excel(
                    excel_writer=writer, 
                    sheet_name=sheet_name,
                    index=False,
                )   
                query_num += 1

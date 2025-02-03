from utils.oracle_db import OracleDB
import logging
import pandas as pd
import os
import datetime as dt
import shutil

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
        sql_joined_lines = " ".join(sql_splitlines)
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
    
    def sql_to_xlsx_with_timestamp(
        self, 
        sql_filepath, 
        xlsx_dir, 
        xlsx_filename_before_timestamp, 
        delete_old_timestamped_xlsx_files=False
    ):
        """
        Runs SQL queries in an .sql file against an Oracle DB and returns the output of each query in 
        an .xlsx file located in xlsx_dir and named in the form of 
        "xlsx_filename_before_timestamp + timestamp.xlsx"

        Args:
            sql_filepath (str): the path to the .sql input file 
            xlsx_dir (str): the directory to the xlsx output file
            xlsx_filename_before_timestamp (str): the name of the xlsx file, which will be appended 
                with a timestamp
            delete_old_timestamped_xlsx_files (bool): True := will delete old timestamped versions of
                the file. False (default) := will not.
        """
        now = dt.datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H-%M-%S")
        xlsx_filename = xlsx_filename_before_timestamp + ' ' + timestamp + '.xlsx'
        xlsx_filepath = xlsx_dir + xlsx_filename
        self.sql_to_xlsx(sql_filepath, xlsx_filepath)
        if delete_old_timestamped_xlsx_files:
            self.delete_old_timestamped_xlsx_files(
                xlsx_dir, xlsx_filename_before_timestamp, xlsx_filename
            )

    def delete_old_timestamped_xlsx_files(
        self, xlsx_dir, xlsx_filename_before_timestamp, xlsx_filename_w_curr_timestamp
    ):
        """
        Deletes timestamped versions of an xlsx file that aren't the latest one.

        Args:
            xlsx_dir (str): the directory to the xlsx file(s)
            xlsx_filename_before_timestamp (str): the names of the xlsx file(s) excluding
                the timestamp that's appended to the end
            xlsx_filename_w_curr_timestamp: the timestamped name of the current xlsx file
                to be left as is
        """
        for (dirpath, dirnames, filenames) in os.walk(xlsx_dir):
            filenames = filenames
        to_index = len(xlsx_filename_before_timestamp)
        for filename in filenames:
            if filename[:to_index] == xlsx_filename_before_timestamp \
                and len(filename) == len(xlsx_filename_w_curr_timestamp) \
                and filename != xlsx_filename_w_curr_timestamp:
                try:
                    os.remove(xlsx_dir + filename)
                except PermissionError:
                    logger.info(f'encountered PermissionError when trying to remove old file version: {filename}')
                    continue

    def copy_file(self, src, dst):
        """
        Copies files from src to dst unless a PermissionError is encountered, in which case
        it does nothing.

        Args:
            src (str): the source file path
            dst (str): the destination file path
        """
        try:
            shutil.copy(src = src, dst = dst)
        except PermissionError:
            pass

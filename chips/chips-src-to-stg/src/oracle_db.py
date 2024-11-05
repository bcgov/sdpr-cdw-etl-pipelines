import oracledb
from src.windows_registry import WindowsRegistry
import src.utils as utils
import pandas as pd
import logging

logger = logging.getLogger('__main__.' + __name__)


class OracleDB:
    """
    A class for managing connections and operations on an Oracle database.

    This class retrieves credentials based on the OS environment and provides methods to 
    execute queries, manage transactions, and perform CRUD operations on Oracle database tables.

    Attributes:
        conn_str_key_endpoint (str): The last part of the Windows Registry key that yields the Oracle connection strings.
        credentials (dict[str, str], optional): A dictionary containing additional credentials.
    """

    def __init__(self, conn_str_key_endpoint: str) -> None:
        """
        Initializes the OracleDB instance.

        Args:
            conn_str_key_endpoint (str): The endpoint for retrieving the Oracle connection string from the registry.
        
        Raises:
            DatabaseException: If a connection to the database cannot be established.
        """
        def connect_w_win_reg():
            reg = WindowsRegistry()
            db_credentials = reg.get_oracle_conn_dict(conn_str_key_endpoint)
            user = db_credentials["user"]
            pwd = db_credentials["pwd"]
            service_name = db_credentials["service_name"]
            self.conn = oracledb.connect(
                user=user,
                password=pwd,
                dsn=f'{service_name}.world',
                config_dir="E:/Oracle/product/18.0.0/64bit/network/admin",
            )
            logger.info("connected to: " + service_name + "." + user)
            self.cursor = self.conn.cursor()
            logger.info("cursor opened")

        try:
            connect_w_win_reg()
        except oracledb.DatabaseError as e:
            raise DatabaseException()

    def open_cursor(self):
        """
        Opens a new cursor for the database connection.

        Returns:
            Cursor: The opened cursor for executing SQL statements.
        """
        self.cursor = self.conn.cursor()
        logger.info("cursor opened")
        return self.cursor

    def close_cursor(self) -> None:
        """
        Closes the current cursor.
        """
        self.cursor.close()
        logger.info("cursor closed")

    def commit(self) -> None:
        """
        Commits the current transaction to the database.
        """
        self.conn.commit()
        logger.debug("committed db changes")

    def close_connection(self) -> None:
        """
        Closes the database connection.
        """
        self.conn.close()
        logger.info("connection closed")

    def commit_and_close(self) -> None:
        """
        Commits the current transaction and closes the database connection.
        """
        self.close_cursor()
        self.commit()
        self.close_connection()

    def execute(self, statement: str, parameters=None) -> None:
        """
        Executes a single SQL statement.

        Args:
            statement (str): The SQL statement to execute.
            parameters (optional): The parameters to bind to the statement.

        Raises:
            DatabaseException: If an error occurs during execution.
            Exception: For any unhandled exceptions.
        """
        # try:
        logger.debug(f'executing "{statement}" with {parameters}')
        self.cursor.execute(statement, parameters)
        # except oracledb.Error as e:
        #     raise DatabaseException(e)
        # except Exception as e:
        #     raise Exception(f"unhandled exception: {e}")

    def execute_many(self, statement: str, parameters) -> None:
        """
        Executes a single SQL statement multiple times with different parameters.

        Args:
            statement (str): The SQL statement to execute.
            parameters: The parameters to bind to the statement for each execution.

        Raises:
            DatabaseException: If an error occurs during execution.
            Exception: For any unhandled exceptions.
        """
        try:
            self.cursor.executemany(statement, parameters)
        except oracledb.Error as e:
            raise DatabaseException(e)
        except Exception as e:
            raise Exception(f"unhandled exception: {e}")

    def query_to_df(self, query_string: str, parameters=None) -> pd.DataFrame:
        """
        Executes a query and returns the result as a pandas DataFrame.

        Args:
            query_string (str): The SQL query to execute.
            parameters (optional): The parameters to bind to the query.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        self.execute(statement=query_string, parameters=parameters)
        data = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def truncate(self, table_owner: str, table_name: str) -> None:
        """
        Truncates a table in the database.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
        """
        statement = f"Truncate table {table_owner}.{table_name} REUSE STORAGE"
        self.execute(statement=statement)

    def default_load(
        self,
        table_owner: str,
        table_name: str,
        insert_cols: str,
        number_of_cols: int,
        writeRows: list[tuple],
        truncate_first: bool = True,
    ) -> None:
        """
        Loads data into a table, optionally truncating it first.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            insert_cols (str): The columns to insert into.
            number_of_cols (int): The number of columns in the insert statement.
            writeRows (list[tuple]): The rows of data to insert.
            truncate_first (bool): Whether to truncate the table before loading data.
        """
        if truncate_first:
            self.truncate(table_owner, table_name)
        self.execute_many(
            statement=f"INSERT INTO {table_owner}.{table_name} {insert_cols} VALUES {utils.bind_vars(number_of_cols)}",
            parameters=writeRows,
        )
        self.commit()

    def grant(self, grant_type: str, on_str: str, to_str: str, parameters=None) -> None:
        """
        Grants privileges of the form: grant {grant_type} on {on_str} to {to_str}

        Args:
            grant_type (str): The grant type.
            on_str (str): The object on which the privileges are being granted.
            to_str (str): The user to which the privileges are granted.
            parameters: The parameters to bind to the statement.
        """
        statement = f"grant {grant_type} on {on_str} to {to_str}"
        self.execute(statement=statement, parameters=parameters)
        self.commit()

    def delete(self, table_owner: str, table_name: str, where_str: str, parameters) -> None:
        """
        Deletes rows from a table based on a condition.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            where_str (str): The condition for deleting rows.
            parameters: The parameters to bind to the condition.
        """
        statement = f"delete from {table_owner}.{table_name} where {where_str}"
        self.execute(statement=statement, parameters=parameters)
        self.commit()

    def add_primary_key(
        self, table_owner: str, table_name: str, pk_name: str, pk_cols: str, parameters=None
    ) -> None:
        """
        Adds a primary key constraint to a table.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            pk_name (str): The name of the primary key constraint.
            pk_cols (str): The columns for the primary key.
            parameters: The parameters to bind to the statement.
        """
        statement = f"alter table {table_owner}.{table_name} add constraint {pk_name} primary key ({pk_cols})"
        self.execute(statement=statement, parameters=parameters)
        self.commit()

    def insert(
        self,
        table_owner: str,
        table_name: str,
        insert_tgt_cols_str: str,
        insert_vals_str: str,
        parameters,
    ) -> None:
        """
        Inserts a new row into a table.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            insert_tgt_cols_str (str): The target columns for the insert.
            insert_vals_str (str): The values to insert.
            parameters: The parameters to bind to the insert statement.
        """
        statement = f"""
            INSERT INTO {table_owner}.{table_name} ({insert_tgt_cols_str}) 
            values ({insert_vals_str})
        """
        self.execute(statement=statement, parameters=parameters)
        self.commit()
    
    def upsert(
        self,
        table_owner: str,
        table_name: str,
        on_str: str,
        update_set_str: str,
        insert_tgt_cols_str: str,
        insert_vals_str: str,
        parameters
    ) -> None:
        """
        Updates or inserts records in a table using a merge statement.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            on_str (str): The condition for matching records.
            update_set_str (str): The columns to update if a match is found.
            insert_tgt_cols_str (str): The target columns for the insert.
            insert_vals_str (str): The values to insert.
            parameters: The parameters to bind to the merge statement.
        """
        statement=f"""
            merge into {table_owner}.{table_name} tgt 
            using dual
            on ({on_str})
            when matched then update set {update_set_str}
            when not matched then insert ({insert_tgt_cols_str}) values ({insert_vals_str})
        """
        self.execute(statement=statement, parameters=parameters)
        self.commit()

    def update(self, table_owner, table_name, set_str, where_str, parameters) -> None:
        """
        Updates records in a table based on a condition.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.
            set_str (str): The columns to update.
            where_str (str): The condition for updating records.
            parameters: The parameters to bind to the update statement.
        """
        statement = f"""
            update {table_owner}.{table_name}
            set {set_str}
            where {where_str}
        """
        self.execute(statement=statement, parameters=parameters)
        self.commit()

    def count_dups_in_oracle(self, table_owner: str, table_name: str) -> int:
        """
        Counts duplicate rows in a table, ignoring columns that cannot be selected.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.

        Returns:
            int: The number of duplicate rows in the table.
        """
        get_cols_in_oracle = self.query_to_df(f"""
            select column_name 
            from all_tab_cols 
            where owner = upper('{table_owner}') 
            and table_name = upper('{table_name}')
            and data_type not in ('CLOB')
        """)
        list_cols_in_oracle = get_cols_in_oracle['COLUMN_NAME'].to_list()
        str_cols_in_oracle = ''
        for col in list_cols_in_oracle:
            str_cols_in_oracle += col
            if col != list_cols_in_oracle[-1]:
                str_cols_in_oracle += ', '

        dups_query = f"""
            select sum(dups) from (
                SELECT {str_cols_in_oracle}, COUNT(*) -1 dups
                FROM {table_owner}.{table_name}
                GROUP BY {str_cols_in_oracle}
                HAVING COUNT(*) > 1
            ) 
        """
        get_duplicates_in_oracle = self.query_to_df(dups_query).iat[0, 0]
        if get_duplicates_in_oracle is None:
            get_duplicates_in_oracle = 0
        else:
            get_duplicates_in_oracle = get_duplicates_in_oracle.item()
        return get_duplicates_in_oracle

    def get_row_count(self, table_owner: str, table_name: str) -> int:
        """
        Gets the total number of rows in a table.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.

        Returns:
            int: The total number of rows in the table.
        """
        row_count = self.query_to_df(f"""
            select count(*) 
            from {table_owner}.{table_name}
        """).iat[0, 0].item()
        return row_count

    def get_col_count(self, table_owner: str, table_name: str) -> int:
        """
        Gets the total number of columns in a table.

        Args:
            table_owner (str): The owner of the table.
            table_name (str): The name of the table.

        Returns:
            int: The total number of columns in the table.
        """
        col_count = (self.query_to_df(f"""
            select count(*) 
            from all_tab_columns 
            where owner = '{table_owner.upper()}' and table_name = '{table_name.upper()}'
        """).iat[0, 0].item())
        return col_count


class DatabaseException(Exception):
    """ 
    Exception raised for errors in the database operations.
    """
    pass


class UnknownEnvironmentException(Exception):
    """ 
    Exception raised for errors related to unknown environment configurations.
    """
    pass

import subprocess
import logging

logger = logging.getLogger('__main__.' + __name__)


class SqlPlus:

    def __init__(self):
        pass

    def run_sqlplus(self, sqlplus_script):
        """
        Run a sql command or group of commands against
        a database using sqlplus.
        """
        p = subprocess.Popen(
            ['sqlplus','/nolog'], 
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        (stdout, stderr) = p.communicate(sqlplus_script.encode('utf-8'))
        stdout_decoded = stdout.decode('utf-8')
        stdout_lines = stdout_decoded.split("\n")

        for line in stdout_lines:
            logger.info(line)

        if "ERROR:" in stdout_decoded:
            raise SqlPlusError(stdout_decoded)
        else:
            return stdout_lines

    def copy(
        self, 
        from_db_user,
        from_db_password,
        from_db_connect_identifier,
        to_db_user,
        to_db_password,
        to_db_connect_identifier,
        command,
        destination_table,
        query,
    ):
        """
        A copy of the form:

        COPY {FROM database | TO database | FROM database TO database} 
        {APPEND|CREATE|INSERT|REPLACE} destination_table [(column, column, column, ...)]
        USING query
        """
        dest_conn_str = f'"{from_db_user}/{from_db_password}@{from_db_connect_identifier}"'
        targ_conn_str = f'"{to_db_user}/{to_db_password}@{to_db_connect_identifier}"'
        statement = f"""
            copy -
            from {dest_conn_str} -
            to {targ_conn_str} -
            {command} {destination_table} using -
            {query}
        """
        logger.info(statement)
        self.run_sqlplus(statement)

    def create_copy(
        self, 
        from_db_user,
        from_db_password,
        from_db_connect_identifier,
        to_db_user,
        to_db_password,
        to_db_connect_identifier,
        target_table,
        destination_table,
    ):
        self.copy(
            from_db_user=from_db_user,
            from_db_password=from_db_password,
            from_db_connect_identifier=from_db_connect_identifier,
            to_db_user=to_db_user,
            to_db_password=to_db_password,
            to_db_connect_identifier=to_db_connect_identifier,
            command='create',
            destination_table=destination_table,
            query=f'select * from {target_table}',
        )

    def replace_copy(
        self, 
        from_db_user,
        from_db_password,
        from_db_connect_identifier,
        to_db_user,
        to_db_password,
        to_db_connect_identifier,
        target_table,
        destination_table,
    ):
        self.copy(
            from_db_user=from_db_user,
            from_db_password=from_db_password,
            from_db_connect_identifier=from_db_connect_identifier,
            to_db_user=to_db_user,
            to_db_password=to_db_password,
            to_db_connect_identifier=to_db_connect_identifier,
            command='replace',
            destination_table=destination_table,
            query=f'select * from {target_table}',
        )

    def create_or_replace_copy(
        self, 
        from_db_user,
        from_db_password,
        from_db_connect_identifier,
        to_db_user,
        to_db_password,
        to_db_connect_identifier,
        target_table,
        destination_table,
    ):
        try:
            self.create_copy(
                from_db_user=from_db_user,
                from_db_password=from_db_password,
                from_db_connect_identifier=from_db_connect_identifier,
                to_db_user=to_db_user,
                to_db_password=to_db_password,
                to_db_connect_identifier=to_db_connect_identifier,
                target_table=target_table,
                destination_table=destination_table,
            )
        except SqlPlusError as e:
            if "ORA-00955: name is already used by an existing object" in str(e):
                self.replace_copy(
                    from_db_user=from_db_user,
                    from_db_password=from_db_password,
                    from_db_connect_identifier=from_db_connect_identifier,
                    to_db_user=to_db_user,
                    to_db_password=to_db_password,
                    to_db_connect_identifier=to_db_connect_identifier,
                    target_table=target_table,
                    destination_table=destination_table,
                )
            else:
                logger.exception(e)
                raise

class SqlPlusError(Exception):
    pass

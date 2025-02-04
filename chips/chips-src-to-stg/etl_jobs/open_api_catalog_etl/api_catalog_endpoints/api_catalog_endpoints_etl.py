import yaml
import pandas as pd
import datetime as dt
import sys
from dotenv import load_dotenv
import os
load_dotenv()
base_dir = os.getenv('PEOPLESOFT_ETL_BASE_DIR')
sys.path.append(base_dir)
from src.peoplesoft_api import PeopleSoftAPI
from src.etl_engine import ETLEngine
main_base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(main_base_dir)
from utils.oracle_db import OracleDB


with open(base_dir + '\\' + 'config.yml', 'r') as file:
    conf = yaml.safe_load(file)


def main():
    db = OracleDB(conn_str_key_endpoint=conf['etl_conn_str_subkey'])
    api = PeopleSoftAPI()
    api.get_catalog()
    endpoints = api.endpoints
    schemas = api.schemas

    data = {
        "endpoint": [],
        "has_matching_table": [],
        "num_rows_at_endpoint": [],
        "num_rows_in_table": [],
        "num_cols_at_endpoint": [],
        "num_cols_in_table": [],
        "last_refresh": [],
    }
    full_tab_df = db.query_to_df(
        "select * from all_tables where owner = 'CHIPS_STG'",
    )
    full_col_df = db.query_to_df("""
        select table_name, count(*) col_count 
        from all_tab_columns 
        where owner = 'CHIPS_STG' 
        group by table_name
    """)
    for endpoint in endpoints:
        f_endpoint = endpoint[1:]
        lookup_table = f_endpoint.upper()
        has_matching_table = "N"
        num_rows_in_table = None
        num_cols_in_table = None
        try:
            tab_df = full_tab_df.query(
                f"TABLE_NAME == '{lookup_table}'",
            ).reset_index()
            col_df = full_col_df.query(
                f"TABLE_NAME == '{lookup_table}'",
            ).reset_index()
            num_rows_in_table = tab_df.loc[0, "NUM_ROWS"]
            num_cols_in_table = col_df.loc[0, "COL_COUNT"]
            has_matching_table = "Y"
            print(f"Table named {lookup_table} exists in CDW")
        except (ValueError, KeyError):
            print(f"Table named {lookup_table} doesn't exist in CDW")
        except Exception:
            print("Unhadled exception occurred")
        data["endpoint"].append(f_endpoint)
        data["has_matching_table"].append(has_matching_table)
        data["num_rows_in_table"].append(num_rows_in_table)
        try:
            record_count = api.get_record_count(f_endpoint)
            data["num_rows_at_endpoint"].append(record_count)
        except:
            print(f"Couldn't get record count for {f_endpoint}")
            data["num_rows_at_endpoint"].append(None)
        data["num_cols_in_table"].append(num_cols_in_table)
        schema = schemas.query(f"endpoint == '{f_endpoint}'")
        data["num_cols_at_endpoint"].append(len(schema))
        data["last_refresh"].append(
            str(dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")),
        )

    df = pd.DataFrame(data)

    db.execute("drop table peoplesoft_api_endpoint purge")
    db.execute("""
		create table peoplesoft_api_endpoint (
			endpoint varchar2(50),
			has_matching_table char(1),
			num_rows_at_endpoint number(9),
			num_rows_in_table number(9),
			num_cols_at_endpoint number(9),
			num_cols_in_table number(9),
			last_refresh date
		)
	""")
    db.execute(
        "comment on column peoplesoft_api_endpoint.endpoint is 'a PeopleSoft API endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_endpoint.has_matching_table is 'Y/N to indicate if there is a table in the CDW that has the same name as the endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_endpoint.num_rows_at_endpoint is 'number of records (rows) available at the endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_endpoint.num_rows_in_table is 'number of rows in the table (as per all_tables)'",
    )
    db.execute(
        "comment on column peoplesoft_api_endpoint.num_cols_at_endpoint is 'number of fields (columns) available at the endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_endpoint.num_cols_in_table is 'number of columns in the table (as per all_tab_cols)'",
    )
    db.execute(
        "comment on column peoplesoft_api_endpoint.last_refresh is 'when the data was last refreshed'",
    )

    etl_engine = ETLEngine(
        api=api,
        api_endpoint="open-api-catalog",
        oracledb=db,
        oracle_table_owner="ETL",
        oracle_table_name="PEOPLESOFT_API_ENDPOINT",
    )

    t = etl_engine.transform(df)
    etl_engine.load(t)


if __name__ == "__main__":
    main()

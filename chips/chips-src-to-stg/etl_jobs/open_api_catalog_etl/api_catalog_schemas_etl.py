import pandas as pd
from datetime import datetime
import yaml
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

    schema = api.schemas
    schema = schema[["endpoint", "field", "oracle_data_type"]]
    schema = schema.rename(columns={"oracle_data_type": "src_data_type"})
    schema["endpoint_upper"] = schema["endpoint"].apply(lambda x: x.upper())
    schema["field_upper"] = schema["field"].apply(lambda x: x.upper())

    full_col_df = db.query_to_df("""
        select TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, 
        NULLABLE, CHAR_LENGTH 
        from all_tab_columns 
        where owner = 'CHIPS_STG'
    """)

    df = schema.merge(
        full_col_df,
        left_on=["endpoint_upper", "field_upper"],
        right_on=["TABLE_NAME", "COLUMN_NAME"],
        how="left",
    )

    def has_matching_tab_col(row):
        if pd.isna(row["COLUMN_NAME"]):
            return "N"
        else:
            return "Y"

    df["has_matching_tab_col"] = df.apply(has_matching_tab_col, axis=1)

    def data_types_match(row):
        if row["src_data_type"] == row["DATA_TYPE"]:
            return "Y"
        else:
            return "N"

    df["data_types_match"] = df.apply(data_types_match, axis=1)

    df["last_refresh"] = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))

    df = df[
        [
            "endpoint",
            "field",
            "has_matching_tab_col",
            "data_types_match",
            "src_data_type",
            "DATA_TYPE",
            "DATA_LENGTH",
            "DATA_SCALE",
            "CHAR_LENGTH",
            "NULLABLE",
            "last_refresh",
        ]
    ]
    df = df.rename(
        columns={
            "DATA_TYPE": "dest_data_type",
            "DATA_LENGTH": "dest_data_length",
            "DATA_SCALE": "dest_data_scale",
            "CHAR_LENGTH": "dest_char_length",
            "NULLABLE": "dest_nullable",
        },
    )

    db.execute("drop table peoplesoft_api_schema purge")
    db.execute("""
		create table peoplesoft_api_schema (
			endpoint varchar2(50),
			field varchar2(100),
			has_matching_tab_col char(1),
			data_types_match char(1),
			src_data_type varchar2(50),
			dest_data_type varchar2(50),
			dest_data_length number(9),
			dest_data_scale number(9),
			dest_char_length number(38),
			dest_nullable varchar2(1),
			last_refresh date
		)
	""")
    db.execute(
        "comment on column peoplesoft_api_schema.endpoint is 'a PeopleSoft API endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.field is 'the endpoint field that maps to a column of the same name in a table that has the same name as endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.has_matching_tab_col is 'indicates if the field maps to a CDW column of the same name in a table of the same name as the endpoint'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.data_types_match is 'indicates if the source data type matches the destination data type'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.src_data_type is 'the data type of the column in the source Oracle DB from which it is retrieved'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.dest_data_type is 'the data type of the column in the destination table in the CDW'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.dest_data_length is 'the data length of the column in the destination table in the CDW'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.dest_data_scale is 'the data scale of the column in the destination table in the CDW'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.dest_char_length is 'the character length of the column in the destination table in the CDW'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.dest_nullable is 'indicates if the column in the destination table in the CDW is nullable'",
    )
    db.execute(
        "comment on column peoplesoft_api_schema.last_refresh is 'when the data was last refreshed'",
    )

    etl_engine = ETLEngine(
        api=api,
        api_endpoint="open-api-catalog",
        oracledb=db,
        oracle_table_owner="ETL",
        oracle_table_name="PEOPLESOFT_API_SCHEMA",
    )

    t = etl_engine.transform(df)
    etl_engine.load(t)


if __name__ == "__main__":
    main()

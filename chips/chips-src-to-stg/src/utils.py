from datetime import datetime
import pandas as pd
import math
import copy


def str_to_datetime(string_date: str) -> datetime:
    """
    Convert a string representation of a date and time to a datetime object.

    This function takes a string in the ISO 8601 format (e.g., 
    "YYYY-MM-DDTHH:MM:SSZ" or "YYYY-MM-DDTHH:MM:SS.ssssssZ") and 
    returns the corresponding datetime object. If the input is 
    None, the function returns None.

    Args:
        string_date (str): The string representation of the date and 
                           time, or None.

    Returns:
        datetime: The corresponding datetime object if the input 
                   string is valid, or None if the input is None.

    Raises:
        ValueError: If the string_date is not in the expected format.
    """
    if string_date is not None:
        try:
            datetime_out = datetime.strptime(string_date, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            datetime_out = datetime.strptime(string_date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                microsecond=0
            )
    else:
        datetime_out = string_date
    return datetime_out


def str_to_int(string_int: str) -> int:
    """
    Convert a string representation of an integer to an int object.

    This function takes a string that represents an integer and returns 
    the corresponding int object. If the input is None, the function 
    returns None.

    Args:
        string_int (str): The string representation of the integer, 
                          or None.

    Returns:
        int: The corresponding int object if the input string is valid, 
             or None if the input is None.

    Raises:
        ValueError: If the string_int cannot be converted to an integer.
    """
    if string_int is not None:
        int_out = int(string_int)
    else:
        int_out = string_int
    return int_out


def str_to_float(string_float: str) -> float:
    """
    Convert a string representation of a float to a float object.

    This function takes a string that represents a floating-point number 
    and returns the corresponding float object. If the input is None, 
    the function returns None.

    Args:
        string_float (str): The string representation of the float, 
                            or None.

    Returns:
        float: The corresponding float object if the input string is 
               valid, or None if the input is None.

    Raises:
        ValueError: If the string_float cannot be converted to a float.
    """
    if string_float is not None:
        float_out = float(string_float)
    else:
        float_out = string_float
    return float_out


def null_to_empty_str(null_val) -> str:
    """
    Convert None to an empty string.

    This function checks if the input is None. If it is, the function 
    returns an empty string; otherwise, it returns the input value.

    Args:
        null_val: The input value, which can be None or any other type.

    Returns:
        str: An empty string if the input is None, or the original 
             input value if it is not None.
    """
    if null_val is None:
        out = ""
    else:
        out = null_val
    return out


def transform_data_type(value, oracle_data_type: str, nullable: str):
    """
    Transform the input value to the corresponding Python type based on the Oracle data type.

    This function takes a value and casts it to the appropriate Python type 
    that fits the Oracle data model. It also considers whether the value 
    is nullable based on the nullable indicator provided.

    Args:
        value: The input value to be transformed.
        oracle_data_type (str): The Oracle data type, such as "VARCHAR2", 
                                "NUMBER", "DATE", etc.
        nullable (str): A flag indicating whether the column is nullable 
                        ("Y" for yes, "N" for no).

    Returns:
        The value casted to the appropriate Python type based on the Oracle 
        data type. If the column is not nullable and the value is None, 
        an empty string is returned.

    Notes:
        - This function assumes that `null_to_empty_str`, `str_to_float`, 
          and `str_to_datetime` are defined elsewhere.
    """
    if nullable == "N":
        value = null_to_empty_str(value)
    if oracle_data_type in ("VARCHAR2", "CHAR", "CLOB"):
        pass  # No transformation needed for these types
    elif oracle_data_type == "NUMBER":
        value = str_to_float(value)
    elif oracle_data_type in ("DATE", "TIMESTAMP", "TIMESTAMP(6)"):
        value = str_to_datetime(value)
    return value


def apply_data_type_transformations(
    data_model_mapping: pd.DataFrame, df: pd.DataFrame
) -> pd.DataFrame:
    """
    Apply data type transformations to a DataFrame based on a mapping.

    This function transforms the columns of the provided DataFrame (`df`) 
    according to the data type specifications in the `data_model_mapping`. 
    Each column is processed based on its corresponding Oracle data type 
    and nullable indicator.

    Args:
        data_model_mapping (pd.DataFrame): A DataFrame containing the mapping 
                                            of source column names, target data 
                                            types, and nullable indicators.
        df (pd.DataFrame): A DataFrame containing data from the API that needs 
                           to be transformed to fit the data model for the 
                           target Oracle table.

    Returns:
        pd.DataFrame: The transformed DataFrame with columns adjusted to fit 
                       the target Oracle data types.

    Notes:
        - This function assumes that the `transform_data_type` function is 
          defined elsewhere.
    """
    for index, row in data_model_mapping.iterrows():
        col_name = row["col_name_src"]
        oracle_data_type = row["data_type_target"]
        nullable = row["nullable_target"]
        df[col_name] = df[col_name].apply(
            lambda val: transform_data_type(val, oracle_data_type, nullable)
        )
    return df


def source_data_model(source_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create a data model DataFrame from the source data.

    This function generates a DataFrame that represents the data model 
    of the input source data, including column names and their corresponding 
    data types.

    Args:
        source_data (pd.DataFrame): The source DataFrame for which the data 
                                     model is to be created.

    Returns:
        pd.DataFrame: A DataFrame containing two columns: "col_name_src" 
                      and "data_type_src", representing the column names 
                      and their respective data types in the source data.

    Notes:
        - The resulting DataFrame can be used for mapping and transforming 
          data types when integrating with other data models.
    """
    model = pd.DataFrame(source_data.dtypes)
    model = model.reset_index()
    model.columns = ["col_name_src", "data_type_src"]
    return model


def target_data_model(owner: str, table_name: str, db) -> pd.DataFrame:
    """
    Retrieve the data model for a specified Oracle table.

    This function queries the Oracle database to obtain the column names, 
    data types, lengths, and nullability information for the specified 
    table owned by the given user.

    Args:
        owner (str): The owner of the Oracle table.
        table_name (str): The name of the Oracle table.
        db: An OracleDB object that provides the method to execute queries 
            and return results as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the data model for the specified 
                      table, with columns:
                      - "col_name_target": The name of the column.
                      - "data_type_target": The data type of the column.
                      - "data_length_target": The length of the column.
                      - "nullable_target": Indicator of whether the column is nullable.

    Notes:
        - This function assumes that the `db` object has a `query_to_df` 
          method that executes the provided SQL query and returns the result 
          as a pandas DataFrame.
    """
    df = db.query_to_df(f"""
        select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
        from sys.dba_tab_columns
        where owner = '{owner}'
          and table_name = '{table_name}'
    """)
    df.columns = [
        "col_name_target",
        "data_type_target",
        "data_length_target",
        "nullable_target",
    ]
    return df


def data_model_mapping(
    source_data_model: pd.DataFrame, target_data_model: pd.DataFrame
) -> pd.DataFrame:
    """
    Create a mapping DataFrame between source and target data models.

    This function compares the source data model and the target data model 
    by merging them based on their column names. The comparison is case-insensitive.

    Args:
        source_data_model (pd.DataFrame): The source data model returned 
                                           by the `source_data_model` function.
        target_data_model (pd.DataFrame): The target data model returned 
                                           by the `target_data_model` function.

    Returns:
        pd.DataFrame: A DataFrame containing the mapping between source and 
                      target data models, with the following columns:
                      - "col_name_src": The name of the source column.
                      - "col_name_target": The name of the target column.
                      - "data_type_src": The data type of the source column.
                      - "data_type_target": The data type of the target column.
                      - "data_length_target": The length of the target column.
                      - "nullable_target": The nullability of the target column.

    Notes:
        - The function performs an outer merge to include all columns from both 
          models, even if there is no direct match.
    """
    source_data_model["upper_col_name_src"] = source_data_model["col_name_src"].str.upper()
    mapping_df = source_data_model.merge(
        target_data_model,
        how="outer",
        left_on="upper_col_name_src",
        right_on="col_name_target",
    )
    mapping_df = mapping_df[
        [
            "col_name_src",
            "col_name_target",
            "data_type_src",
            "data_type_target",
            "data_length_target",
            "nullable_target",
        ]
    ]
    return mapping_df


def data_model_discrepancies(data_model_mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Identify discrepancies in the data model mapping.

    This function analyzes the data model mapping DataFrame to find 
    columns that are missing from either the source or target data model. 
    It returns a DataFrame containing only the discrepancies.

    Args:
        data_model_mapping (pd.DataFrame): The data model mapping DataFrame 
                                            returned by the `data_model_mapping` function.

    Returns:
        pd.DataFrame: A DataFrame containing rows where either the source 
                      column or the target column is missing (NaN).

    Notes:
        - Discrepancies indicate columns that do not have a corresponding match 
          in the other data model.
    """
    df = data_model_mapping[
        data_model_mapping["col_name_src"].isna() | data_model_mapping["col_name_target"].isna()
    ]
    return df


def data_model_consistencies(data_model_mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Identify consistent entries in the data model mapping.

    This function analyzes the data model mapping DataFrame to find 
    columns that are present in both the source and target data models. 
    It returns a DataFrame containing only the consistent entries.

    Args:
        data_model_mapping (pd.DataFrame): The data model mapping DataFrame 
                                            returned by the `data_model_mapping` function.

    Returns:
        pd.DataFrame: A DataFrame containing rows where both the source 
                      column and the target column are present (not null).

    Notes:
        - Consistencies indicate columns that have a corresponding match 
          in both data models, allowing for further validation or processing.
    """
    df = data_model_mapping[
        data_model_mapping["col_name_src"].notnull() &
        data_model_mapping["col_name_target"].notnull()
    ]
    df = df.reset_index(drop=True)
    return df


def is_nan(value) -> bool:
    """
    Check if a value is NaN (Not a Number).

    This function checks if the provided value is NaN, including cases 
    where a NumPy NaN has been converted to a standard Python NaN type 
    (`float('nan')`). It returns True if the value is NaN, and False otherwise.

    Args:
        value: The value to check for NaN.

    Returns:
        bool: True if the value is NaN, False otherwise.

    Notes:
        - This function attempts to convert the value to a float. If the 
          conversion raises a ValueError, the function returns False.
    """
    try:
        return math.isnan(float(value))
    except ValueError:
        return False


def equate_col_names(
    data_model_mapping: pd.DataFrame, col_name_src: str, col_name_target: str
) -> pd.DataFrame:
    """
    Reconcile differences in source and target column names in a data model mapping.

    This function updates the data model mapping DataFrame to reflect the 
    equivalence of a source column name and a target column name when 
    their data matches, despite differing names.

    Args:
        data_model_mapping (pd.DataFrame): The data model mapping DataFrame.
        col_name_src (str): The source column name.
        col_name_target (str): The target column name.

    Returns:
        pd.DataFrame: The updated data model mapping DataFrame with reconciled 
                      source and target columns.

    Notes:
        - This function modifies the input DataFrame and drops old source 
          field rows that have been merged into the target column row.
    """
    # get the value of the source data type
    data_type_src = data_model_mapping.query(f'col_name_src == "{col_name_src}"')[
        "data_type_src"
    ].item()

    # create a new data model mapping and merge the associated source and target rows
    new_dm_mapping = data_model_mapping
    new_dm_mapping.loc[new_dm_mapping["col_name_target"] == col_name_target, "data_type_src"] = (
        data_type_src
    )
    new_dm_mapping.loc[new_dm_mapping["col_name_target"] == col_name_target, "col_name_src"] = (
        col_name_src
    )

    # drop the old source field row now that it's been merged into the target column row
    for index, row in new_dm_mapping.iterrows():
        if is_nan(row["col_name_target"]) and row["col_name_src"] == col_name_src:
            new_dm_mapping = new_dm_mapping.drop(index)

    return new_dm_mapping


def insert_cols_str(cols: list[str]) -> str:
    """
    Format a list of column names for SQL insert statements.

    This function takes a list of column names and formats them into a 
    string suitable for use in an SQL insert statement.

    Args:
        cols (list[str]): A list of column names.

    Returns:
        str: A formatted string of column names, e.g., "(col1, col2, ...)".
    """
    insert_cols = "("
    for col in cols:
        insert_cols += col + ", "
    insert_cols = insert_cols[:-2] + ")"
    return insert_cols


def timedelta_to_nearest_sec(timedelta: datetime) -> str:
    """
    Convert a timedelta to a string in the format HH:MM:SS.

    This function takes a timedelta object and converts it into a string 
    representation that includes hours, minutes, and seconds.

    Args:
        timedelta (datetime): A timedelta object.

    Returns:
        str: A string formatted as "HH:MM:SS".
    """
    parts = str(timedelta).split(":")
    hours = parts[0]
    mins = parts[1]
    secs = str(round(float(parts[2])))
    if len(secs) == 1:
        secs = "0" + secs
    return hours + ":" + mins + ":" + secs


def flatten(list_of_lists):
    """
    Flatten a list of lists into a single list.

    This function takes a list of lists and returns a single list 
    containing all the elements from the nested lists.

    Args:
        list_of_lists: A list of lists.

    Returns:
        list: A flattened list containing all elements from the input lists.
    """
    return [item for sublist in list_of_lists for item in sublist]


def chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst.

    Args:
        lst (list): The list to be divided into chunks.
        n (int): The size of each chunk.

    Yields:
        list: Subsequent chunks of the specified size.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def split_int(an_int: int) -> list[int]:
    """
    Split an integer into two integers that add up to it.

    If the integer is even, it returns a list of the form 
    [an_int / 2, an_int / 2]. If odd, it returns 
    [an_int / 2 - 0.5, an_int / 2 + 0.5].

    Args:
        an_int (int): The integer to split.

    Returns:
        list[int]: A list containing two integers that sum to an_int.
    """
    if an_int % 2 == 0:
        val = an_int / 2
        split = [int(val), int(val)]
    elif an_int % 2 == 1:
        val = an_int / 2
        split = [int(val - 0.5), int(val + 0.5)]
    return split


def split_page_size_params(params: dict) -> list[dict]:
    """
    Split pagination parameters into two sets with halved limits.

    This function takes a dictionary of request parameters containing 
    'limit' and 'offset' and returns two dictionaries with the limit 
    halved, adjusting the second offset accordingly.

    Args:
        params (dict): A dictionary containing 'limit' and 'offset' keys.

    Returns:
        list[dict]: A list containing two dictionaries with updated 
                     pagination parameters.
    """
    limit = params['limit']
    offset = params['offset']
    new_limits = split_int(limit)
    second_offset = offset + new_limits[0]

    params_a = copy.deepcopy(params)
    params_a['limit'] = new_limits[0]
    params_a['offset'] = offset

    params_b = copy.deepcopy(params)
    params_b['limit'] = new_limits[1]
    params_b['offset'] = second_offset

    return [params_a, params_b]


def merge_two_dicts(x, y):
    """
    Merge two dictionaries into a new dictionary.

    This function creates a shallow copy of the first dictionary and 
    updates it with key-value pairs from the second dictionary.

    Args:
        x (dict): The first dictionary.
        y (dict): The second dictionary.

    Returns:
        dict: A new dictionary containing the merged contents of x and y.
    """
    z = x.copy()
    z.update(y)
    return z

    
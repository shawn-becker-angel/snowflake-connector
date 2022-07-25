import os
import pandas as pd
from typing import Set, List, Tuple, Any, Optional
import datetime
import snowflake.connector as connector
from utils import is_readable_file, find_latest_file, generate_random_string
from pprint import pprint
from constants import *

# Returns True if df is None or has zero rows
def is_empty_data_frame(df: pd.DataFrame) -> bool:
    return True if df is None or len(df) == 0 else False

# Returns zero if df is empty otherwise return the number of rows
def get_data_frame_len(df: pd.DataFrame) -> int:
    return 0 if is_empty_data_frame(df) else len(df)

# Saves the DataFrame (sets column ID as index column) 
# to a new data_file with the given base_name, current timestamp and 
# file format extension (CSV_FORMAT | FEATHER_FORMAT) and 
# returns the name of the new data_file
def save_data_frame(base_name: str, df: pd.DataFrame, format: str=CSV_FORMAT) -> str:
    df = df.set_index(keys=["ID"])
    utc_now = datetime.datetime.utcnow().isoformat()
    data_file = f"/tmp/{base_name}-{utc_now}.{format}"
    if format == CSV_FORMAT:
        df.to_csv(data_file) 
    elif format == FEATHER_FORMAT: 
        df.to_feather(data_file)
    return data_file

# Uses the data_file's file format extension (CSV_FORMAT | FEATHER_FORMAT)
# to load the DataFrame (sets column ID as index column) or None if the given data_file
# is not found or is not readable
def load_data_frame(data_file: str) -> Optional[pd.DataFrame]:
    if is_readable_file(data_file):
        if data_file.endswith(CSV_FORMAT):
            df = pd.read_csv(data_file)
        elif data_file.endswith(FEATHER_FORMAT):
            df = pd.read_feather(data_file)
    if df is not None:
        df = df.set_index(keys=["ID"])
    return df

# Returns the data_file and the data_frame if the latest data_file
# with the given base_name is found, is readable and is decodablea. 
# Otherwise returns None
def load_latest_data_frame(base_name: str) -> Optional[Tuple[str, pd.DataFrame]]:
    data_file = find_latest_file(f"/tmp/{base_name}-*.*")
    df = load_data_frame(data_file)
    return (data_file, df) if df is not None else None

# Return a DataFrame created as num_rows dicts with num_cols key-value pairs
def generate_random_dataframe(num_rows: int=3, num_cols: int=3) -> pd.DataFrame:
    cols = []
    for i in range(num_cols):
        cols.append("col:" + generate_random_string())
    rows = []
    for r in range(num_rows):
        row = {}
        for c in range(num_cols):
            row[cols[c]] = f"data-{r}-{c}:" + generate_random_string()
        rows.append(row)
    df = pd.DataFrame(data=rows, columns=cols)
    return df

    
################################################
# Tests
################################################

def test_save_load_dataframe():
    saved_df = generate_random_dataframe()
    base_name = "dataframe_utils_test"
    
    saved_csv_file = save_data_frame(base_name, saved_df)
    (loaded_csv_file, loaded_df) = load_latest_data_frame(base_name)
    
    assert loaded_csv_file == saved_csv_file, f"ERROR: expected\n{saved_csv_file} not\n{loaded_csv_file}"
    saved_df_str = saved_df.to_string()
    loaded_df_str = loaded_df.to_string()
    assert loaded_df_str == saved_df_str, f"ERROR: expected\n{saved_df_str} not\n{loaded_df_str}"


def tests():
    test_save_load_dataframe()
    print("all tests passed in", os.path.basename(__file__))


def main():
    tests()

if __name__ == "__main__":
    main()
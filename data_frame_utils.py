import os
import pandas as pd
from typing import Set, List, Tuple, Any, Optional
import datetime
import snowflake.connector as connector
from utils import is_readable_file, find_latest_file, generate_random_string
from pprint import pprint

# Returns True if df is None or has zero rows
def is_empty_data_frame(df: pd.DataFrame) -> bool:
    return True if df is None or len(df) == 0 else False

# Returns zero if df is empty otherwise return the number of rows
def get_data_frame_len(df: pd.DataFrame) -> int:
    return 0 if is_empty_data_frame(df) else len(df)

# Saves the DataFrame (without index column) to a new timestamped 
# base_name csv file and returns the new csv_file
def save_data_frame(base_name: str, df: pd.DataFrame) -> str:
    utc_now = datetime.datetime.utcnow().isoformat()
    csv_file = f"/tmp/{base_name}-{utc_now}.csv"
    df.to_csv(csv_file, index=False)
    return csv_file

# Finds the latest csv file for the given base_name and returns the csv_file 
# and the loaded DataFrame (without index column) or None if latest csv
# file not found or is not readable
def load_latest_data_frame(base_name: str) -> Optional[Tuple[str, pd.DataFrame]]:
    csv_file = find_latest_file(f"/tmp/{base_name}-*.csv")
    if is_readable_file(csv_file):
        df = pd.read_csv(csv_file, index_col=False)
        return (csv_file, df)
    return None


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
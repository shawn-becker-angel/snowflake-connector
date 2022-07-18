import os
import sys
import datetime
from numpy import save
import pandas as pd
from typing import List, Dict, Tuple
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from constants import *   
from query_generator import query_batch_generator, create_connector
from utils import find_latest_file, is_readable_file
from data_frame_utils import is_empty_data_frame, get_data_frame_len, load_latest_data_frame, save_data_frame

# Returns a list of ellis_island_users, each of which is a 
# dict with ELLIS_ISLAND_USER_COLUMNS attributes
def compute_ellis_island_users_df(conn: connector=None, verbose: bool=True) -> pd.DataFrame:
    ellis_island_users = []
    dot_freq = 10000
    dot_char = '.'
    
    query = \
"""select U.UUID AS USER_UUID, U.USERNAME AS USER_USERNAME, TO_VARCHAR(A.DATA:EMAIL) AS USER_EMAIL 
    FROM STITCH_LANDING.ELLIS_ISLAND.USER U 
    join STITCH_LANDING.ELLIS_ISLAND.SOCIAL_AUTH A 
    ON A.USER_ID = U.ID
    WHERE U.INSERTED_AT > '2021-04-01'
"""
    if verbose:
        print("query:", query)
    query_batch_iterator = query_batch_generator(query, conn=conn)
    num_users = 0
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for batch_row in batch_rows:
                
                ellis_island_user = dict(zip(ELLIS_ISLAND_USERS_DF_COLUMNS, batch_row))
                ellis_island_users.append(ellis_island_user)
                
                if num_users % dot_freq == 0:
                    sys.stdout.write(dot_char)
                    sys.stdout.flush()

                num_users += 1
        except StopIteration:
            break
    print()
    ellis_island_users_df = pd.DataFrame(data=ellis_island_users, columns=ELLIS_ISLAND_USERS_DF_COLUMNS)
    ellis_island_users_df = ellis_island_users_df.drop_duplicates(keep="first")
    return ellis_island_users_df

# Returns an ellis_island_users_df either 
# loaded from the latest csv file
# or newly computed and saved to a new csv file
def get_ellis_island_users_df(conn: connector=None, load_latest: bool=True, verbose: bool=True) -> Tuple[str, pd.DataFrame]:
    csv_file = None
    users_df = None
    if load_latest:
        loaded = load_latest_data_frame(ELLIS_ISLAND_USERS_DF_DEFAULT_BASE_NAME)
        if loaded is not None:
            (csv_file, users_df) = loaded
            if verbose:
                print(f"loaded {get_data_frame_len(users_df):,} ellis_island_users from {csv_file}")
            
    if is_empty_data_frame(users_df):
        if verbose:
            print(f"compute ellis_island_users")
        users_df = compute_ellis_island_users_df(conn=conn)
        csv_file = save_data_frame(ELLIS_ISLAND_USERS_DF_DEFAULT_BASE_NAME, users_df)
        if verbose:
            print(f"saved {get_data_frame_len(users_df):,} ellis_island_users to {csv_file}")

    return (csv_file, users_df)

################################################
# Tests
################################################

def test_get_ellis_island_users_df():
    conn = create_connector()
    (users_csv_file, users_df) = get_ellis_island_users_df(conn=conn, load_latest=False)
    assert len(users_df) > 0, f"ERROR: zero ellis_island_users"

def tests():
    test_get_ellis_island_users_df()

def main(): 
    tests()

if __name__ == "__main__":
    main()
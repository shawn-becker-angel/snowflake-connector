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
def compute_ellis_island_users_df(conn: connector=None) -> pd.DataFrame:
    ellis_island_users = []
    dot_freq = 10000
    dot_char = '.'
    query = \
"""select u.uuid, u.username, u.inserted_at, u.updated_at, TO_VARCHAR(a.data:email) 
    FROM STITCH_LANDING.ELLIS_ISLAND.USER u 
    join STITCH_LANDING.ELLIS_ISLAND.SOCIAL_AUTH a 
    on a.user_id = u.id
"""
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
    ellis_island_users_df = pd.DataFrame(data=ellis_island_users, columns=ELLIS_ISLAND_USERS_DF_COLUMNS)
    return ellis_island_users_df

# Returns a ellis_island_users_df either loaded from the latest csv file
# or a newly computed and saved ellis_island_users_df
#  
def get_ellis_island_users_df(conn: connector=None, load_latest: bool=True, verbose: bool=False) -> pd.DataFrame:
    ellis_island_users_df = None
    base_name = ELLIS_ISLAND_USERS_DF_DEFAULT_BASE_NAME
    # attempt to load ellis_island_users_df from the most recent csv file
    if load_latest:
        result = load_latest_data_frame(base_name)
        if result:
            _, ellis_island_users_df = result
    # compute and save a new ellis_island_users_df if needed
    if not ellis_island_users_df:
        ellis_island_users_df = compute_ellis_island_users_df(conn=conn)
    return ellis_island_users_df

# Returns the csv file and the DataFrame created 
# from a list of ellis island user dicts
def save_ellis_island_users(ellis_island_users: List[Dict]) -> Tuple[str, pd.DataFrame]:
    users_df = pd.DataFrame(data=ellis_island_users, columns=ELLIS_ISLAND_USERS_DF_COLUMNS)
    utc_now = datetime.datetime.utcnow().isoformat()
    users_csv_file = f"/tmp/ellis_island_users-{utc_now}.csv"
    users_df.to_csv(users_csv_file)
    return (users_csv_file, users_df)

# Returns an ellis_island_users csv file and DataFrame either by 
# loading the latest csv file or by computing and saving a new one.
def get_ellis_island_users_df(conn: connector=None, use_latest: bool=True, verbose: bool=True) -> Tuple[str, pd.DataFrame]:
    csv_file = None
    users_df = None
    if use_latest:
        result = load_latest_data_frame(ELLIS_ISLAND_USERS_DF_DEFAULT_BASE_NAME)
        if result is not None:
            (csv_file, users_df) = result
        if verbose:
            print(f"loaded {get_data_frame_len(users_df)} ellis_island_users from {csv_file}")
            
    if is_empty_data_frame(users_df):
        if verbose:
            print(f"compute ellis_island_users")
        users_df = compute_ellis_island_users_df(conn=conn)
        csv_file = save_data_frame(ELLIS_ISLAND_USERS_DF_DEFAULT_BASE_NAME, users_df)
        if verbose:
            print("saved {get_data_frame_len(users_df)} ellis_island_users to {csv_file}")

    return (csv_file, users_df)

################################################
# Tests
################################################

def test_get_ellis_island_users_df():
    conn = create_connector()
    (users_csv_file, users_df) = get_ellis_island_users_df(conn=conn)
    assert len(users_df) > 0, f"ERROR: zero ellis_island_users"

def tests():
    test_get_ellis_island_users_df()

def main(): 
    tests()

if __name__ == "__main__":
    main()
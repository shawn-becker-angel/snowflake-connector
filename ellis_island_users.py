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
from utils import find_latest_file, is_readable_file, is_empty_df

ELLIS_ISLAND_SEGMENT_TABLE = "STITCH_LANDING.ELLIS_ISLAND.USER"
ELLIS_ISLAND_USER_COLUMNS = ["uuid","username","inserted_at","updated_at"]

# Returns a list of ellis_island_users, each of which is a 
# dict with ELLIS_ISLAND_USER_COLUMNS attributes
def compute_ellis_island_users(conn: connector=None) -> List[Dict]:
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
                
                ellis_island_user = dict(zip(ELLIS_ISLAND_USER_COLUMNS, batch_row))
                ellis_island_users.append(ellis_island_user)
                
                if num_users % dot_freq == 0:
                    sys.stdout.write(dot_char)
                    sys.stdout.flush()

                num_users += 1
        except StopIteration:
            break
    return ellis_island_users

# Returns the csv file and the DataFrame created 
# from a list of ellis island user dicts
def save_ellis_island_users(ellis_island_users: List[Dict]) -> Tuple[str, pd.DataFrame]:
    users_df = pd.DataFrame(data=ellis_island_users, columns=ELLIS_ISLAND_USER_COLUMNS)
    utc_now = datetime.datetime.utcnow().isoformat()
    users_csv_file = f"/tmp/ellis_island_users-{utc_now}.csv"
    users_df.to_csv(users_csv_file)
    return (users_csv_file, users_df)

# Returns an ellis_island_users csv file and DataFrame either by 
# loading the latest csv file or by computing and saving a new one.
def get_ellis_island_users(conn: connector=None) -> Tuple[str, pd.DataFrame]:
    users_csv_file = None
    users_df = None
    if USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE:
        users_csv_file = find_latest_file(pattern="/tmp/ellis_island_users-*.csv")
        if is_readable_file(users_csv_file):
            print(f"load ellis_island_users")
            users_df = pd.read_csv(users_csv_file)
            
    if users_df is None:
        print(f"compute ellis_island_users")
        users = compute_ellis_island_users(conn=conn)
        
        print("save ellis_island_users")
        (users_csv_file, users_df) = save_ellis_island_users(users)
        
    return (users_csv_file, users_df)

################################################
# Tests
################################################

def test():
    conn = create_connector()
    
    print("compute_ellis_island_users")
    users = compute_ellis_island_users(conn=conn)
    
    print("save_ellis_island_users")
    (users_csv_file, users_df) = save_ellis_island_users(users)
    print("\ncomputed num ellis_island_users:", len(users_df), "saved to:", users_csv_file)

def main(): 
    test()

if __name__ == "__main__":
    main()
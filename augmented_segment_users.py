import sys
import os
import glob
import math
import pandas as pd
from pandasql import sqldf
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import Set, List, Dict, Optional, Tuple
import datetime
from functools import cache
from query_generator import create_connector, query_batch_generator
from utils import is_empty_list, is_empty_df
from ellis_island_users import get_ellis_island_users


# Returns new augmented_segment_users_df with columns ['USER_ID', 'EMAIL', 'UUID'] by
# doing a left outer join with ellis_island_users_df.
# NOTE that the new 'UUID' column may be None
def compute_augmented_segment_users_df(segment_table: str, ellis_island_users_df: pd.DataFrame):
    segment_select_clause = ",".join(ALL_KEY_COLUMNS_LIST)
    segment_where_clause = " and ".join([f"{x} is not NULL" for x in ALL_KEY_COLUMNS_LIST])
    segment_table_query = f"SELECT {segment_select_clause} from {segment_table} WHERE {segment_where_clause}"
    
    users_df = ellis_island_users_df
    users_df.rename(columns = {'UUID':'USER_UUID', 'EMAIL':'USER_EMAIL', 'USERNAME':'USER_USERNAME'}, inplace = True)

    augmented_df = None
    segment_batch_iterator = query_batch_generator(segment_table_query)
    while True:
        try:
            segment_rows = next(segment_batch_iterator)
            segment_df = pd.DataFrame(data=segment_rows, columns=ALL_KEY_COLUMNS_LIST)
            
            user_uuid_joined_df = pd.merge(left=segment_df, right=users_df, how="left", left_on='USER_ID', right_on='USER_UUID')
            user_email_joined_df = pd.merge(left=segment_df, right=users_df, how="left", left_on='EMAIL', right_on='USER_EMAIL')
            user_username_joined_df = pd.merge(left=segment_df, right=users_df, how="left", left_on='EMAIL', right_on='USER_USERNAME')
            
            union_df = pd.union([user_uuid_joined_df, user_email_joined_df, user_username_joined_df])
            
            assert len(union_df) == len(segment_df), "ERROR: df length failure"
            
            augmented_df = union_df if is_empty_df(augmented_df) else pd.union([augmented_df, union_df])

        except StopIteration:
            break
    
    return augmented_df

# Print stats of # rows with or without valid UUID for 
# each segment_table with all key_columns
@timefunc
def get_augmented_segment_users(segment_tables: List[str]):
    if not is_empty_list(segment_tables):
        print("num segment_tables:", len(segment_tables))

        print("get_ellis_island_users")
        (users_csv_file, users_df) = get_ellis_island_users()
        if not is_empty_df(users_df):
            print("num ellis_island_users:", len(users_df), "in:", users_csv_file)
            
            for segment_table in segment_tables:
                print("compute_augmented_segment_users_df")
                augmented_segment_users_df = compute_augmented_segment_users_df(segment_table, users_df)
                if not is_empty_df(augmented_segment_users_df):
                    num_null_UUID_rows = sum([True for idx,row in augmented_segment_users_df.iterrows() if row['UUID'].isnull()])
                    num_valid_UUID_rows = sum([True for idx,row in augmented_segment_users_df.iterrows() if ~row['UUID'].isnull()])
                    assert num_null_UUID_rows + num_valid_UUID_rows == len(augmented_segment_users_df), f"ERROR: row count failure for segment_table:{segment_table}"
                    print("segment_table: {segment_table} num_null_UUID_rows: {num_null_UUID_rows} num_valid_UUID_rows:{num_valid_UUID_rows}")
                else:
                    print(f"segment_table: {segment_table} has zero augmented_segement_users")
        else:
            print("zero ellis_island_users")
    else:
        print("zero segment_tables")


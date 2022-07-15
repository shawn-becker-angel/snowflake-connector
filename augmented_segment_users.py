import sys
import os
import glob
import math
import pandas as pd
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
from segment_tables import get_segment_tables


# Returns new augmented_segment_table_users_df with columns 
# ['TABLE_NAME','ANONYMOUS_ID','USER_ID', 'EMAIL', 'USER_UUID'] by
# doing a left outer join with ellis_island_users_df to get USER_UUID.
# NOTE that the new 'USER_UUID' column may be None
def compute_augmented_segment_table_users(segment_table: str, ellis_island_users_df: pd.DataFrame, conn: connector=None):

    augmented_df_columns = ['SEGMENT_TABLE',*ALL_KEY_COLUMNS_LIST,'USER_UUID']
    augmented_users_df = pd.DataFrame(columns=augmented_df_columns)

    ellis_island_users_df.rename(columns = {'UUID':'USER_UUID', 'EMAIL':'USER_EMAIL', 'USERNAME':'USER_USERNAME'}, inplace = True)

    segment_table_users_columns = ALL_KEY_COLUMNS_LIST
    segment_table_users_select_clause = ",".join(segment_table_users_columns)
    segment_table_users_where_clause = " and ".join([f"{x} is not NULL" for x in segment_table_users_columns])
    segment_table_users_query = f"SELECT DISTINCT {segment_table_users_select_clause} from {segment_table} WHERE {segment_table_users_where_clause}"
    
    segment_table_users_batch_iterator = query_batch_generator(segment_table_users_query, conn)
    num_batches = 0
    dot_frequency = 10
    dot = '+'

    while True:
        try:
            segment_table_users_batch = next(segment_table_users_batch_iterator)
            segment_table_users_batch_df = pd.DataFrame(data=segment_table_users_batch, columns=segment_table_users_columns)
            segment_table_users_batch_df['SEGMENT_TABLE'] = segment_table

            user_uuid_joined_df = pd.merge(left=segment_table_users_batch_df, right=ellis_island_users_df, how="left", left_on='USER_ID', right_on='USER_UUID')
            user_uuid_joined_df = user_uuid_joined_df[augmented_df_columns]
            user_uuid_joined_df.drop_duplicates(keep="first", inplace=True)
            
            assert len(user_uuid_joined_df) == len(segment_table_users_batch_df), \
                f"ERROR: user_uuid_joined_df length expected {len(segment_table_users_batch_df)} not {len(user_uuid_joined_df)}"
            
            augmented_users_df = pd.concat([augmented_users_df, user_uuid_joined_df], axis=0)
            
            if num_batches % dot_frequency == 0:
                sys.stdout.write(dot)
                sys.stdout.flush()
            num_batches += 1

        except StopIteration:
            break
        
    print("\ncreated",len(augmented_users_df),"augmented users for segment_table",segment_table,"in", num_batches,"batches")
    return augmented_users_df

# Print stats of # rows with or without valid UUID for 
# each segment_table with all key_columns
@timefunc
def compute_augmented_segment_users(segment_tables: List[str], ellis_island_users_df: pd.DataFrame=None, conn: connector=None):
    
    if not is_empty_list(segment_tables):
        print("num segment_tables:", len(segment_tables))
        
        if ellis_island_users_df is None:
            print("get_ellis_island_users")
            (_, ellis_island_users_df) = get_ellis_island_users(conn)

        if not is_empty_df(ellis_island_users_df):
            print("num ellis_island_users:", len(ellis_island_users_df))
            
            for segment_table in segment_tables:
                assert isinstance(segment_table, str), "ERROR: instance error"
                
                print("compute_augmented_segment_table_users segment_table:", segment_table)
                augmented_segment_table_users_df = compute_augmented_segment_table_users(segment_table, ellis_island_users_df,conn)
    
                if not is_empty_df(augmented_segment_table_users_df):
                    df = augmented_segment_table_users_df
                    total_users = len(df)
                    null_UUIDs = df['USER_UUID'].isna().sum()
                    nnull_UUIDs = df['USER_UUID'].notna().sum()
                    assert null_UUIDs + nnull_UUIDs == len(df), f"ERROR: row count failure for segment_table:{segment_table}"
                    null_perc = 100.0 * null_UUIDs / total_users
                    nnull_perc = 100.0 - null_perc
                    print(f"segment_table: {segment_table} total users:{total_users} null_UUIDs: {null_UUIDs} ({null_perc:5.2f}%) not_null_UUIDs:{nnull_UUIDs} ({nnull_perc:5.2f}%)")
                else:
                    print(f"segment_table: {segment_table} total users: 0")
        else:
            print("zero ellis_island_users")
    else:
        print("zero segment_tables")

################################################
# Tests
################################################

def test():
    conn = create_connector()
    
    print("get_segment_tables")
    (csv, segment_tables_df) = get_segment_tables(conn=conn)
    segment_tables = list(segment_tables_df['segment_table'].values)
    
    print("get_ellis_island_users")
    (csv, ellis_users_df) = get_ellis_island_users(conn=conn)
    
    print("compute_augmented_segment_users")
    compute_augmented_segment_users(segment_tables, ellis_users_df,conn=conn)

def main():
    test()

if __name__ == "__main__":
    main()
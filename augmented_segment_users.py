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
from utils import is_empty_list
from data_frame_utils import is_empty_data_frame
from ellis_island_users import get_ellis_island_users_df
from segment_tables import get_segment_tables_df, find_segment_table_columns
from data_frame_utils import save_data_frame


# Augments segment_users of one segment_table_dict by adding a vetted ellis_island_user UUID.
# This function Iteratively runs left outer joins of small batches of segment users of
# the given segment_table_dict against the HUGE ellis_island_users_df and concatonates the 
# result of each join into a single augmented_segment_users_df for that segment_table.
def compute_augmented_segment_users_df(segment_table_dict: Dict[str,str], ellis_island_users_df: pd.DataFrame, verbose: bool=True, conn: connector=None) -> pd.DataFrame:

    # create an empty df to hold all concatonated join results for this segment_table_dict
    augmented_users_df = pd.DataFrame(columns=AUGMENTED_SEGMENT_USERS_DF_COLUMNS)
    
    # WARNING: THIS AFFECTS THE ORIGINAL DataFrame!!
    # rename the ellis_island_users_df columns for the joins against segment_users in this segment_table_dict
    ellis_island_users_df.rename(columns = {'UUID':'USER_UUID', 'EMAIL':'USER_EMAIL', 'USERNAME':'USER_USERNAME'}, inplace = True)

    # define the shape of the segment_table_user_df batches
    segment_table = segment_table_dict['segment_table']
    segment_table_users_columns = segment_table_dict['columns'].split("-")
    segment_table_users_select_clause = ",".join(segment_table_users_columns)
    segment_table_users_where_clause = " and ".join([f"{x} is not NULL" for x in segment_table_users_columns])
    segment_table_users_query = f"SELECT DISTINCT {segment_table_users_select_clause} from {segment_table} WHERE {segment_table_users_where_clause}"
    
    #
    # ITERATIVELY join small segment_table_user_df batches against the HUGE ellis_island_users_df
    #
    segment_table_users_batch_iterator = query_batch_generator(segment_table_users_query, conn)
    num_batches = 0
    dot_frequency = 10
    dot = '+'

    while True:
        try:
            segment_table_users_batch = next(segment_table_users_batch_iterator)
            segment_table_users_batch_df = pd.DataFrame(data=segment_table_users_batch, columns=segment_table_users_columns)
            segment_table_users_batch_df['SEGMENT_TABLE'] = segment_table

            # do the join to get the ellis_island USER_UUID in user_uuid_joined_df
            user_uuid_joined_df = pd.merge(left=segment_table_users_batch_df, right=ellis_island_users_df, how="left", left_on='USER_ID', right_on='USER_UUID')
            user_uuid_joined_df = user_uuid_joined_df[AUGMENTED_SEGMENT_USERS_DF_COLUMNS]
            user_uuid_joined_df.drop_duplicates(keep="first", inplace=True)
            
            assert len(user_uuid_joined_df) == len(segment_table_users_batch_df), \
                f"ERROR: user_uuid_joined_df length expected {len(segment_table_users_batch_df)} not {len(user_uuid_joined_df)}"
            
            # concat the user_uuid_joined_df of this batch into the full augmented_users_df
            augmented_users_df = pd.concat([augmented_users_df, user_uuid_joined_df], axis=0)
            
            if num_batches % dot_frequency == 0:
                sys.stdout.write(dot)
                sys.stdout.flush()
            num_batches += 1

        except StopIteration:
            break
    
    if verbose:
        print("\ncreated",len(augmented_users_df),"augmented users for segment_table",segment_table,"in", num_batches,"batches")
    return augmented_users_df



# Print stats of # rows with or without valid UUID for 
# each segment_table with all key_columns
@timefunc
def compute_augmented_segment_users_for_all_segment_tables(verbose: bool=True, conn: connector=None) -> None:
    
    if verbose:
        print("get_segment_tables_df")
    (_, segment_tables_df) = get_segment_tables_df(conn=conn)
    segment_table_dicts = list(segment_tables_df.values)
    if verbose:
        print("num segment_table_dicts:", len(segment_table_dicts))
    for segment_table_dict in segment_table_dicts:
        if verbose:
            print("get_ellis_island_users_df")
        (_, ellis_island_users_df) = get_ellis_island_users_df(conn=conn)
        if verbose:
            print("num ellis_island_users:", len(ellis_island_users_df))

        if verbose:
            print("compute_augmented_segment_users_df for segment_table_dict:", segment_table_dict)
        augmented_segment_users_df = compute_augmented_segment_users_df(segment_table_dict, ellis_island_users_df, conn)

        segment_table = segment_table_dict['segment_table']

        if not is_empty_data_frame(augmented_segment_users_df):
            df = augmented_segment_users_df
            total_users = len(df)
            null_UUIDs = df['USER_UUID'].isna().sum()
            nnull_UUIDs = df['USER_UUID'].notna().sum()
            assert null_UUIDs + nnull_UUIDs == len(df), f"ERROR: row count failure for segment_table:{segment_table}"
            null_perc = 100.0 * null_UUIDs / total_users
            nnull_perc = 100.0 - null_perc

            if verbose:
                print(f"augmented_segment_users_df for segment_table: {segment_table} total users:{total_users} null_UUIDs: {null_UUIDs} ({null_perc:5.2f}%) not_null_UUIDs:{nnull_UUIDs} ({nnull_perc:5.2f}%)")
            csv_file = save_data_frame(AUGMENTED_SEGMENT_USERS_DF_DEFAULT_BASE_NAME, augmented_segment_users_df)
            if verbose:
                print(f"augmented_segment_users_df for segment_table: {segment_table} saved to {csv_file}")

        else:
            if verbose:
                print(f"segment_table: {segment_table} total users: 0")
    # for segment_table_dicts


################################################
# Tests
################################################

def test():
    conn = create_connector()
    
    print("get_segment_tables_df")
    (_, segment_tables_df) = get_segment_tables_df(conn=conn)
    
    print("get_ellis_island_users")
    (_, ellis_users_df) = get_ellis_island_users_df(conn=conn)
    
    print("compute_augmented_segment_users_for_all_segment_tables")
    compute_augmented_segment_users_for_all_segment_tables(sconn=conn)

def main():
    test()

if __name__ == "__main__":
    main()
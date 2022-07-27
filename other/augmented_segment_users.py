import sys
import pandas as pd
import snowflake.connector as connector
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import Dict
from functools import cache
from query_generator import create_connector, query_batch_generator, execute_count_query
from utils import is_empty_list
from data_frame_utils import is_empty_data_frame
from ellis_island_users import get_ellis_island_users_df
from segment_tables import get_segment_tables_df, get_segment_table_dicts
from data_frame_utils import save_data_frame, get_data_frame_len, is_empty_data_frame, load_latest_data_frame

AUGMENTED_SEGMENT_USERS_DF_DEFAULT_BASE_NAME = "augmented_segment_users_df"

# Augments segment_users of one segment_table_dict by adding a vetted ellis_island_user UUID.
# This function Iteratively runs left outer joins of small batches of segment users of
# the given segment_table_dict against the HUGE ellis_island_users_df and concatonates the 
# result of each join into a single augmented_segment_users_df for that segment_table.
def compute_augmented_segment_users_df(
    segment_table_dict: Dict[str,str], 
    ellis_island_users_df: pd.DataFrame, 
    verbose: bool=True, 
    conn: connector=None) -> pd.DataFrame:
    
    # check the shape of the ellis_island_users_df
    assert not is_empty_data_frame(ellis_island_users_df), "ERROR: empty ellis_island_users_df"
    assert set(ellis_island_users_df.columns) == set(ELLIS_ISLAND_USERS_DF_COLUMNS), "ERROR: unmatched ellis_island_users_df columns"

    # define the shape of the segment_table_user_df batches
    segment_table = segment_table_dict['segment_table']
    segment_users_columns = set(segment_table_dict['columns'].split("-"))
    
    segment_users_columns = list( segment_users_columns)
    users_select_clause = ",".join(segment_users_columns)
    users_not_null_clause = " and ".join([f"{x} is not NULL" for x in segment_users_columns])
    segment_users_from_where_clause = f" from {segment_table} WHERE {users_not_null_clause}"
    segment_users_select_query = f"SELECT DISTINCT {users_select_clause} {segment_users_from_where_clause}"
    segment_users_count_query = f"SELECT COUNT (DISTINCT {users_select_clause}) {segment_users_from_where_clause}"
    
    # create an empty augmented_users_df to hold all concatonated join results for this segment_table_dict
    augmented_segment_users_columns = [*segment_users_columns, "USER_UUID"]
    augmented_users_df = pd.DataFrame(columns=augmented_segment_users_columns)
    
    # check the size of the segment_users
    num_segment_users = execute_count_query(segment_users_count_query, conn=conn)
    if verbose:
        print(f"num_segment_users:{num_segment_users:,}")

    # iteratively join small segment_table_user_df batches against the HUGE ellis_island_users_df
    batch_size = 100
    segment_users_batch_iterator = query_batch_generator(segment_users_select_query, conn=conn, batch_size=batch_size)
    num_batches = 0
    dot_frequency = 10
    dot = '+'

    while True:
        try:
            segment_users_batch = next(segment_users_batch_iterator)
            
            # convert the batch into a data frame
            segment_users_batch_df = pd.DataFrame(data=segment_users_batch, columns=segment_users_columns)
            
            # drop duplicate segment_user rows
            pre_dd_len = len(segment_users_batch_df)
            segment_users_batch_df = segment_users_batch_df.drop_duplicates(keep="first")
            post_dd_len = len(segment_users_batch_df)
            
            if verbose and post_dd_len != pre_dd_len:
                print(f"segment_users_batch_df pre_dd_len:{pre_dd_len} post_dd_len:{post_dd_len}")
                
            if post_dd_len > 0:
                # do the join to get the ellis_island USER_UUID in user_uuid_joined_df
                user_uuid_joined_df = pd.merge(left=segment_users_batch_df, right=ellis_island_users_df, how="left", left_on='USER_ID', right_on='USER_UUID')
                
                # keep only the required columns
                user_uuid_joined_df = user_uuid_joined_df[augmented_segment_users_columns]
                            
                # drop duplicate rows
                user_uuid_joined_df = user_uuid_joined_df.drop_duplicates(keep="first")

                assert len(user_uuid_joined_df) == len(segment_users_batch_df), \
                    f"ERROR: user_uuid_joined_df length expected {len(segment_users_batch_df)} not {len(user_uuid_joined_df)}"

                # note that user_uuid_joined_df['USER_UUID'].isna() is retained for later reporting
                
                # vertically concat the user_uuid_joined_df batch into the full augmented_users_df, if needed
                if get_data_frame_len(user_uuid_joined_df) > 0:
                    augmented_users_df = pd.concat([augmented_users_df, user_uuid_joined_df], axis=0)
            
            if num_batches % dot_frequency == 0:
                sys.stdout.write(dot)
                sys.stdout.flush()
            num_batches += 1

        except StopIteration:
            break
    
    # finally add this column
    augmented_users_df['SEGMENT_TABLE'] = segment_table
    if verbose:
        print(f"\ncreated {get_data_frame_len(augmented_users_df):,} augmented users for segment_table {segment_table} in {num_batches} batches")
    return augmented_users_df

# Print stats of # rows with or without valid UUID for 
def report_augmented_segment_users_df_stats(segment_table, augmented_segment_users_df):
    df = augmented_segment_users_df
    total_users = len(df)
    null_UUIDs = df['USER_UUID'].isna().sum()
    nnull_UUIDs = df['USER_UUID'].notna().sum()
    assert null_UUIDs + nnull_UUIDs == len(df), f"ERROR: row count failure for segment_table:{segment_table}"
    null_perc = 100.0 * null_UUIDs / total_users
    nnull_perc = 100.0 - null_perc
    print(f"augmented_segment_users_df for segment_table {segment_table} #total:{total_users:,} #null_UUIDs:{null_UUIDs:,} ({null_perc:5.2f}%) #not_null_UUIDs:{nnull_UUIDs:,} ({nnull_perc:5.2f}%)")


# compute and save an augmented_segment_users_df for all segment_tables against the same ellis_island_users_df
@timefunc
def compute_and_save_augmented_segment_users_df_for_all_segment_tables(verbose: bool=True, conn: connector=None) -> None:
    if verbose:
        print("get_ellis_island_users_df")
    (_, ellis_island_users_df) = get_ellis_island_users_df(conn=conn)
    
    if verbose:
        print("get_segment_tables_df")
    (_, segment_tables_df) = get_segment_tables_df(conn=conn, load_latest=True)
    
    # converts 2-column dataframe to a list of 2-property dicts
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    if verbose:
        print("num segment_table_dicts:", len(segment_table_dicts))
    
    # traverse all segment_table_dicts
    for segment_table_dict in segment_table_dicts:
        
        segment_table = segment_table_dict['segment_table']
        base_name = f"{segment_table}_{AUGMENTED_SEGMENT_USERS_DF_DEFAULT_BASE_NAME}"

        augmented_segment_users_df = None
        csv_file = None
        
        # attempt to load latest augmented_segment_users_df and report stats
        loaded = load_latest_data_frame(base_name)
        if loaded is not None:
            (csv_file, augmented_segment_users_df) = loaded
        if not is_empty_data_frame(augmented_segment_users_df):
            if verbose:
                print(f"loaded augmented_segment_users_df for segment_table {segment_table} loaded from {csv_file}")
                report_augmented_segment_users_df_stats(segment_table, augmented_segment_users_df)
                
        # otherwise, compute new augmented_segment_users_df, report stats and save to csv
        else: 
            if verbose:
                print("compute_augmented_segment_users_df for segment_table_dict:", segment_table_dict)
            
            augmented_segment_users_df = compute_augmented_segment_users_df(segment_table_dict, ellis_island_users_df, conn=conn)

            if not is_empty_data_frame(augmented_segment_users_df):
                if verbose:
                    report_augmented_segment_users_df_stats(segment_table, augmented_segment_users_df)

                # save the augmented_segment_users_df for this sdgment_table
                csv_file = save_data_frame(base_name, augmented_segment_users_df)
                if verbose:
                    print(f"computed augmented_segment_users_df for segment_table {segment_table} saved to {csv_file}")

            else:
                if verbose:
                    print(f"computed augmented_segment_users_df: {segment_table} total users: 0")
    # for segment_table_dict


################################################
# Tests
################################################

def test():
    conn = create_connector()
        
    print("compute_and_save_augmented_segment_users_df_for_all_segment_tables")
    compute_and_save_augmented_segment_users_df_for_all_segment_tables(conn=conn)

def main():
    test()

if __name__ == "__main__":
    main()
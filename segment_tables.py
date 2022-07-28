import os
import sys
import pandas as pd
from requests import get
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from query_generator import query_batch_generator, create_connector, execute_batched_select_query, execute_single_query, execute_count_query, execute_simple_query
from utils import matches_any, find_latest_file, is_readable_file
from functools import cache
import datetime
from timefunc import timefunc
from segment_utils import get_segment_table_from_metadata_table, get_metadata_table_from_segment_table
from pprint import pprint
from data_frame_utils import save_data_frame, load_latest_data_frame, is_empty_data_frame
from pandas.testing import assert_frame_equal

# Returns the set of all queryable segment_tables that have all key_columns as well as 
# any optinal ALL_SEARCH_COLUMNS as a data_frame. 
# A segment_table_dict has structure:
# {
#     'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES',
#     'metadata_table': 'SEGMENT__ANGEL_APP_IOS__IDENTIFIES',
#     'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
# }

def compute_segment_table_dicts_df(conn: connector=None, verbose: bool=True) -> pd.DataFrame:
    segment_table_dicts = []
        
    segment_table_column_sets = {} 
        
    query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
    if verbose:
        print("compute_segment_table_dicts_df query:\n", query)

    query_batch_iterator = query_batch_generator(query, timeout_seconds=15, conn=conn)

    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
        
                # metadata_table: e.g. SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
                metadata_table = result_row[0]
                column_name = result_row[1]
                data_type = result_row[2]

                # segment_table: e.g. SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
                segment_table = get_segment_table_from_metadata_table(metadata_table)
                
                if matches_any(segment_table, SEARCH_SEGMENT_TABLES_SET) and \
                    not matches_any(segment_table, SEARCH_IGNORE_SEGMENT_TABLES_SET):
                        
                    if column_name in ALL_SEARCH_COLUMNS:
                        columns_set = segment_table_column_sets.get(segment_table, set())
                        columns_set.add(column_name)
                        segment_table_column_sets[segment_table] = columns_set
                      
        except StopIteration:
            break
        
    # add a new segment_table_dict to the list of segment_table_dicts 
    # if it's columns_set has ALL_KEY_COLUMNS
    for segment_table, columns_set in segment_table_column_sets.items():
        if columns_set >= ALL_KEY_COLUMNS:
            
            # create a segment_table_dict
            segment_table_dict = {}
            segment_table_dict['segment_table'] = segment_table  
            columns_str = "-".join([x for x in columns_set])
            segment_table_dict['columns'] = columns_str
            metadata_table = get_metadata_table_from_segment_table(segment_table)
            segment_table_dict['metadata_table'] = metadata_table
            
            # append segment_table_dict to segment_table_dicts
            segment_table_dicts.append(segment_table_dict)

    if verbose:
        print()
        print(len(segment_table_dicts), "segment_table_dicts")
        for segment_table_dict in segment_table_dicts:
            pprint(segment_table_dict)
    
    # add the metadata_table property to each 
    # create a DataFrame from the list of segment_table dictionaries
    segment_tables_df = pd.DataFrame(data=segment_table_dicts, columns=SEGMENT_TABLE_DICTS_DF_COLUMNS)
    return segment_tables_df

# Returns the latest data_file and df of a recomputed and auto-saved segment_tables
def compute_and_save_new_segment_table_dicts_df(verbose: bool=True) -> Tuple[str, pd.DataFrame]:
    new_df = compute_segment_table_dicts_df()
    saved_data_file = save_data_frame(SEGMENT_TABLE_DICTS_DF_DEFAULT_BASE_NAME, new_df, PARQUET_FORMAT)
    (latest_data_file,  latest_df) = get_segment_table_dicts_df(load_latest=True)
    assert latest_data_file == saved_data_file, f"ERROR: expected:{saved_data_file} not:{latest_data_file}"
    assert_frame_equal(latest_df, new_df)
    return (latest_data_file, latest_df)

# Returns a list of dicts for each row from a multi-column data_frame
def get_segment_table_dicts(segment_tables_dicts_df: pd.DataFrame) -> List[Dict[str,str]]:
    segment_table_dicts = [dict(zip(list(segment_tables_dicts_df.columns), list(row))) for row in segment_tables_dicts_df.values]
    return segment_table_dicts

# Returns a segment_table_dicts_df either loaded from the latest data_file
# or a newly computed and saved segments_table_dicts_df
#  
def get_segment_table_dicts_df(base_name: str=None, conn: connector=None, load_latest: bool=True, verbose: bool=False) -> Tuple[str,pd.DataFrame]:
    segment_table_dicts_df = None
    data_file = None
    if base_name is None:
        base_name = SEGMENT_TABLE_DICTS_DF_DEFAULT_BASE_NAME
    # attempt to load segments_table_df from the most recent data_file
    if load_latest:
        loaded = load_latest_data_frame(base_name)
        if loaded is not None:
            data_file, segment_table_dicts_df = loaded

    # compute and save a new segment_tables_df if needed
    if is_empty_data_frame(segment_table_dicts_df):
        segment_table_dicts_df = compute_segment_table_dicts_df(conn=conn)
        data_file = save_data_frame(base_name, segment_table_dicts_df, PARQUET_FORMAT)

    return [data_file, segment_table_dicts_df]

# Queries snowflake to return a list of column names for the given segment_table
def find_segment_table_columns(segment_table: str, conn: connector=None, verbose: bool=False) -> List[str]:
    
    # convert segment_table to metadata_table
    metadata_table = get_metadata_table_from_segment_table(segment_table)
    
    select_columns = ['COLUMN_NAME']
    select_str =  ','.join(select_columns)

    select_query = f"SELECT {select_str} FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{metadata_table}'"
    
    segment_table_columns_df = execute_batched_select_query(select_query, select_columns, conn=conn, batch_size=100, timeout_seconds=5, verbose=verbose)
    return list(segment_table_columns_df.values)

################################################
# Tests
################################################

def test_compute_and_save_new_segment_table_dicts_df():
    data_file, new_df = compute_and_save_new_segment_table_dicts_df()
    
def test_compute_save_load_get_new_segment_table_dicts_df():
    new_df = compute_segment_table_dicts_df()
    
    test_base_name = "test_compute_save_load_get_new_segment_table_dicts_df"
    saved_data_file = save_data_frame(test_base_name, new_df, PARQUET_FORMAT)
    
    # verify load_latest_data_frame
    (loaded_data_file, loaded_df) = load_latest_data_frame(test_base_name)
    assert loaded_data_file == saved_data_file, f"ERROR: expected:{saved_data_file} not:{loaded_data_file}"
    assert_frame_equal(loaded_df, new_df)
    
    # verify get_segment_table_dicts_df load_latest=True
    (latest_data_file, latest_df) = get_segment_table_dicts_df(test_base_name, load_latest=True)
    assert latest_data_file == loaded_data_file, f"ERROR: expected:{loaded_data_file} not:{latest_data_file}"
    assert_frame_equal(latest_df, loaded_df)
        
def test_find_segment_table_columns():
    segment_table = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF'
    columns = find_segment_table_columns(segment_table)
    assert len(columns) > 0, f"ERROR: no columns found for segment_table: {segment_table}"

def tests():
    test_compute_and_save_new_segment_table_dicts_df()
    test_find_segment_table_columns()
    
    print()
    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()
    
if __name__ == "__main__":
    main()

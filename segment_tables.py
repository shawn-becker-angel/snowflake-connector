import os
import sys
import pandas as pd
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from query_generator import query_batch_generator, create_connector
from utils import matches_any, find_latest_file, is_readable_file
from functools import cache
import datetime
from timefunc import timefunc
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from key_column_infos import get_key_column_infos
from segment_utils import get_segment_table_from_info_schema_table_name, get_info_schema_table_name_from_segment_table
from pprint import pprint
from data_frame_utils import save_data_frame, load_latest_data_frame
from pandas.testing import assert_frame_equal

# Returns the set of all queryable segment_tables that have all key_columns
def compute_segment_tables_df(conn: connector=None, verbose: bool=False) -> pd.DataFrame:
    
    dot_freq = 10000
    dot_char = '.'
    
    # keeps the set of significant columns for each segment_table
    # { segment_table:<segment_table>, columns_set: (<key_column>...) }
    segment_table_column_sets = {} 
        
    query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
    if verbose:
        print("compute_segment_tables_df query:\n", query)

    query_batch_iterator = query_batch_generator(query, timeout_seconds=15, conn=conn)

    total_rows = 0
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
        
                # table__name: e.g. SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
                info_schema_table_name = result_row[0]
                # segment_table: e.g. SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
                segment_table = get_segment_table_from_info_schema_table_name(info_schema_table_name)

                column_name = result_row[1]
                
                action = 'o' # column skipped
                if column_name in ALL_SEARCH_COLUMNS and \
                    matches_any(segment_table, SEARCH_SEGMENT_TABLES_SET) and \
                    not matches_any(segment_table, SEARCH_IGNORE_SEGMENT_TABLES_SET):
                    columns_set = segment_table_column_sets.get(segment_table, set())
                    columns_set.add(column_name)
                    segment_table_column_sets[segment_table] = columns_set
                    action = '+' # column added
              
                if verbose:      
                    print(action, result_row)  
                else: 
                    if total_rows % dot_freq == 0:
                        sys.stdout.write(action)
                        sys.stdout.flush()
 
                total_rows += 1                
        
        except StopIteration:
            break
    
    assert SEGMENT_TABLES_DF_COLUMNS == 'segment_table-columns', "ERROR: SEGMENT_TABLES_DF_COLUMNS failure"

    # create the list of segment_table_dict filtered according to their column_sets
    segment_tables = list()
    for segment_table, columns_set in segment_table_column_sets.items():
        segment_table_dict = { 'segment_table': segment_table }        
        if columns_set >= ALL_SEARCH_COLUMNS:
            # only a few segment_tables have the more restrictive 'all_search_columns' property
            segment_table_dict['columns'] = ALL_SEARCH_COLUMNS_STR
        elif columns_set >= ALL_KEY_COLUMNS:
            # all segment_tables have the less restrictive 'all_key_columns' property
            segment_table_dict['columns'] = ALL_KEY_COLUMNS_STR
        segment_tables.append(segment_table_dict)

    if verbose:
        print()
        print(len(segment_tables), "segment_tables")
        for segment_table_dict in segment_tables:
            pprint(segment_table_dict)

    # create a DataFrame from the list of segment_table dictionaries
    segment_tables_df = pd.DataFrame(data=segment_tables, columns=SEGMENT_TABLES_DF_COLUMNS)

    return segment_tables_df


# Returns a segment_table_df either loaded from the latest csv file
# or a newly computed and saved segments_table_df
#  
def get_segment_tables_df(conn: connector=None, load_latest: bool=True, verbose: bool=False) -> pd.DataFrame:
    segment_tables_df = None
    base_name = SEGMENT_TABLES_DF_DEFAULT_BASE_NAME
    # attempt to load segments_table_df from the most recent csv file
    if load_latest:
        result = load_latest_data_frame(base_name)
        if result:
            _, segment_tables_df = result
    # compute and save a new segment_tables_df if needed
    if not segment_tables_df:
        segment_tables_df = compute_segment_tables_df(conn=conn)
    return segment_tables_df


# Queries snowflake to return a list of column names for the given segment_table
def find_segment_table_columns(segment_table: str, conn: connector=None, verbose: bool=False) -> List[str]:
    
    # convert segment_table to info_schema_table_name
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    
    # select all column_names from table
    query = f"SELECT COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{info_schema_table_name}'"
    if verbose:
        print("segment_table_has_columns query:\n", query)
    
    query_batch_iterator = query_batch_generator(query, timeout_seconds=5, conn=conn)
    columns_found = set()
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
                column_name = result_row[0]
                columns_found.add(column_name)
        except StopIteration:
            break
    return columns_found

################################################
# Tests
################################################

def test_get_segment_tables_df():
    segment_tables_df = get_segment_tables_df()
    assert len(segment_tables_df) > 0
    
def test_compute_save_load_new_segment_tables_df():
    saved_df = compute_segment_tables_df()
    
    base_name = "test_compute_save_load_new_segment_tables_df"
    saved_csv_file = save_data_frame(base_name, saved_df)
    (loaded_csv_file, loaded_df) = load_latest_data_frame(base_name)
    
    assert loaded_csv_file == saved_csv_file, f"ERROR: expected:{saved_csv_file} not:{loaded_csv_file}"
    assert_frame_equal(loaded_df, saved_df)
        
        
def test_find_segment_table_columns():
    segment_table = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF'
    columns = find_segment_table_columns(segment_table)
    assert len(columns) > 0, f"ERROR: no columns found for segment_table: {segment_table}"

def tests():
    test_get_segment_tables_df()
    test_find_segment_table_columns()
    test_compute_save_load_new_segment_tables_df()
    
    print()
    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()
    
if __name__ == "__main__":
    main()

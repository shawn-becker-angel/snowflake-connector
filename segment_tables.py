import os
import sys
import pandas as pd
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from query_generator import query_batch_generator, create_connector, execute_batched_select_query
from utils import matches_any, find_latest_file, is_readable_file
from functools import cache
import datetime
from timefunc import timefunc
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from key_column_infos import get_key_column_infos
from segment_utils import get_segment_table_from_info_schema_table_name, get_info_schema_table_name_from_segment_table
from pprint import pprint
from data_frame_utils import save_data_frame, load_latest_data_frame, is_empty_data_frame
from pandas.testing import assert_frame_equal

# return the first timestamp column in the given columns set
def get_first_timestamp_column_in_column_set(columns_set: Set[str]) -> Optional[str]:
    timestamp_columns_set = columns_set.intersection(ALL_TIMESTAMP_COLUMNS)
    first_timestamp_column = sorted(list(timestamp_columns_set))[0] if len(timestamp_columns_set) > 0 else None
    return first_timestamp_column
    
# keep only the first timestamp column in the given columns set
def remove_extra_timestamp_columns_in_column_set(columns_set: Set[str]) -> Set[str]:
    timestamp_columns_set = columns_set.intersection(ALL_TIMESTAMP_COLUMNS)
    first_timestamp_column = sorted(list(timestamp_columns_set))[0] if len(timestamp_columns_set) > 0 else None
    if first_timestamp_column is not None and len(timestamp_columns_set) > 0:
        unused_timestamp_columns = timestamp_columns_set - set([first_timestamp_column])
        columns_set = columns_set - unused_timestamp_columns
    return columns_set

# Returns the set of all queryable segment_tables that have all key_columns
def compute_segment_tables_df(conn: connector=None, verbose: bool=True) -> pd.DataFrame:
    
    dot_freq = 10000
    dot_char = '.'
    
    # keeps the set of significant columns for each segment_table
    # { segment_table:<segment_table>, columns_set: (<key_column>...) }
    segment_table_column_sets = {} 
        
    query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
    if verbose:
        print("compute_segment_tables_df query:\n", query)

    query_batch_iterator = query_batch_generator(query, timeout_seconds=15, conn=conn)

    total_rows = 0
    timestamp_column_name_counter = {}
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
        
                # table__name: e.g. SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
                info_schema_table_name = result_row[0]
                # segment_table: e.g. SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
                segment_table = get_segment_table_from_info_schema_table_name(info_schema_table_name)

                column_name = result_row[1]
                data_type = result_row[2]
                
                action = 'o' # column skipped
                if matches_any(segment_table, SEARCH_SEGMENT_TABLES_SET) and \
                    not matches_any(segment_table, SEARCH_IGNORE_SEGMENT_TABLES_SET):

                    if data_type.upper() == 'TIMESTAMP_NTZ':
                        count = timestamp_column_name_counter.get(column_name,0)
                        timestamp_column_name_counter[column_name] = count+1
                        
                    if column_name in ALL_SEARCH_COLUMNS:
                        columns_set = segment_table_column_sets.get(segment_table, set())
                        columns_set.add(column_name)
                        segment_table_column_sets[segment_table] = columns_set
                        action = '+' # column added
              
                if action != 'o':
                    if verbose:      
                        print(action, result_row)  
                    else: 
                        if total_rows % dot_freq == 0:
                            sys.stdout.write(action)
                            sys.stdout.flush()
 
                total_rows += 1                
        
        except StopIteration:
            break
    
    # report the top 10 set of timestamp column_names
    ordered = dict(sorted(timestamp_column_name_counter.items(), key=lambda item: item[1],reverse=True))
    print("\ntimestamp_column_name_counter:", ordered)
    
    # create the list of segment_table_dict keeping only those that meet column requirements
    segment_tables = list()
    for segment_table, columns_set in segment_table_column_sets.items():
        columns_set = remove_extra_timestamp_columns_in_column_set(columns_set)
        columns_str = "-".join([x for x in columns_set])
        segment_table_dict = { 'segment_table': segment_table }        
        if columns_set >= ALL_KEY_COLUMNS:
            segment_table_dict['columns'] = columns_str
            segment_tables.append(segment_table_dict)

    if verbose:
        print()
        print(len(segment_tables), "segment_tables")
        for segment_table_dict in segment_tables:
            pprint(segment_table_dict)

    # create a DataFrame from the list of segment_table dictionaries
    segment_tables_df = pd.DataFrame(data=segment_tables, columns=SEGMENT_TABLES_DF_COLUMNS)
    return segment_tables_df

# Returns the result of converting a 2-column dataframe to a list of 2-property dicts
def get_segment_table_dicts(segment_tables_df: pd.DataFrame) -> List[Dict[str,str]]:
    segment_table_dicts = [dict(zip(list(segment_tables_df.columns), list(row))) for row in segment_tables_df.values]
    return segment_table_dicts

# Returns a segment_table_df either loaded from the latest csv file
# or a newly computed and saved segments_table_df
#  
def get_segment_tables_df(base_name: str=None, conn: connector=None, load_latest: bool=True, verbose: bool=False) -> pd.DataFrame:
    segment_tables_df = None
    csv_file = None
    if base_name is None:
        base_name = SEGMENT_TABLES_DF_DEFAULT_BASE_NAME
    # attempt to load segments_table_df from the most recent csv file
    if load_latest:
        loaded = load_latest_data_frame(base_name)
        if loaded is not None:
            csv_file, segment_tables_df = loaded

    # compute and save a new segment_tables_df if needed
    if is_empty_data_frame(segment_tables_df):
        segment_tables_df = compute_segment_tables_df(conn=conn)
        csv_file = save_data_frame(base_name, segment_tables_df)

    return segment_tables_df

# Queries snowflake to return a list of column names for the given segment_table
def find_segment_table_columns(segment_table: str, conn: connector=None, verbose: bool=False) -> List[str]:
    
    # convert segment_table to info_schema_table_name
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    
    select_columns = ['COLUMN_NAME']
    select_str =  ','.join(select_columns)

    select_query = f"SELECT {select_str} FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{info_schema_table_name}'"
    
    segment_table_columns_df = execute_batched_select_query(select_query, select_columns, conn=conn, batch_size=100, timeout_seconds=5, verbose=verbose)
    return list(segment_table_columns_df.values)
    

################################################
# Tests
################################################
    
    
def test_compute_save_load_get_new_segment_tables_df():
    saved_df = compute_segment_tables_df()
    
    base_name = "test_compute_save_load_new_segment_tables_df"
    saved_csv_file = save_data_frame(base_name, saved_df)
    
    (loaded_csv_file, loaded_df) = load_latest_data_frame(base_name)
    
    assert loaded_csv_file == saved_csv_file, f"ERROR: expected:{saved_csv_file} not:{loaded_csv_file}"
    assert_frame_equal(loaded_df, saved_df)
    
    (latest__csv, latest_df) = get_segment_tables_df(base_name, load_latest=True)
    assert_frame_equal(latest_df, saved_df)

def test_find_segment_table_columns():
    segment_table = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF'
    columns = find_segment_table_columns(segment_table)
    assert len(columns) > 0, f"ERROR: no columns found for segment_table: {segment_table}"

def test_timestamp_columns_in_column_set():
    columns_set = ALL_SEARCH_COLUMNS
    columns_set = remove_extra_timestamp_columns_in_column_set(columns_set)
    v = ALL_SEARCH_COLUMNS - ALL_TIMESTAMP_COLUMNS
    v.add("RECEIVED_AT")
    assert columns_set == v
    
    first_timesamp_column = get_first_timestamp_column_in_column_set(columns_set)
    assert first_timesamp_column == "RECEIVED_AT"


def tests():
    test_timestamp_columns_in_column_set()
    test_find_segment_table_columns()
    # test_compute_save_load_get_new_segment_tables_df()
    
    print()
    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()
    
if __name__ == "__main__":
    main()

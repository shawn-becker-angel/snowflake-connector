import os
import sys
import pandas as pd
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from query_generator import query_batch_generator, create_connector, execute_batched_select_query, execute_single_query, execute_count_query, execute_simple_query
from utils import matches_any, find_latest_file, is_readable_file
from functools import cache
import datetime
from timefunc import timefunc
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from segment_utils import get_segment_table_from_info_schema_table_name, get_info_schema_table_name_from_segment_table
from pprint import pprint
from data_frame_utils import save_data_frame, load_latest_data_frame, is_empty_data_frame
from pandas.testing import assert_frame_equal

# Returns the set of all queryable segment_tables that have all key_columns as well as any optinal ALL_SEARCH_COLUMNS
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

# Returns the latest csv and df of a recomputed and auto-saved segment_tables
def compute_and_save_new_segment_tables_df(verbose: bool=True) -> Tuple[str, pd.DataFrame]:
    saved_df = compute_segment_tables_df()
    saved_csv_file = save_data_frame(SEGMENT_TABLES_DF_DEFAULT_BASE_NAME, saved_df)
    (latest_csv, latest_df) = get_segment_tables_df(load_latest=True)
    assert_frame_equal(latest_df, saved_df)
    return (latest_csv, latest_df)

# Returns a list of 2-property dicts from a 2-column dataframe
def get_segment_table_dicts(segment_tables_df: pd.DataFrame) -> List[Dict[str,str]]:
    segment_table_dicts = [dict(zip(list(segment_tables_df.columns), list(row))) for row in segment_tables_df.values]
    return segment_table_dicts

# Returns a segment_table_df either loaded from the latest csv file
# or a newly computed and saved segments_table_df
#  
def get_segment_tables_df(base_name: str=None, conn: connector=None, load_latest: bool=True, verbose: bool=False) -> Tuple[str,pd.DataFrame]:
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

    return [csv_file, segment_tables_df]

# Queries snowflake to return a list of column names for the given segment_table
def find_segment_table_columns(segment_table: str, conn: connector=None, verbose: bool=False) -> List[str]:
    
    # convert segment_table to info_schema_table_name
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    
    select_columns = ['COLUMN_NAME']
    select_str =  ','.join(select_columns)

    select_query = f"SELECT {select_str} FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{info_schema_table_name}'"
    
    segment_table_columns_df = execute_batched_select_query(select_query, select_columns, conn=conn, batch_size=100, timeout_seconds=5, verbose=verbose)
    return list(segment_table_columns_df.values)

def clone_segment_tables(segment_tables_df, conn: connector=None, verbose:bool=True, preview_only: bool=True) -> List[str]:
    show_tables_columns = ['created_on','name','database_name','schema_name	kind','comment','cluster_by	rows','bytes','owner','retention_time','automatic_clustering','change_tracking','search_optimization','search_optimization_progress','search_optimization_bytes','is_external']
    show_tables_query = "show tables like '%IDENTIFIES' in SEGMENT.IDENTIFIES_METADATA"
    results = execute_simple_query(show_tables_query)
    existing_info_schema_table_names = []
    for line in results:
        line_dict = dict(zip(show_tables_columns, line))
        existing_info_schema_table_names.append(f"{line_dict['name']}")
    
    newly_cloned_tables = []
    for row in segment_tables_df.values:
        segment_table = row[0]
        info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
        if info_schema_table_name not in existing_info_schema_table_names:
            cloned_table = f"SEGMENT.IDENTIFIES_METADATA.{info_schema_table_name}"
            clone_query = f"create table if not exists {cloned_table} clone {segment_table}"
            if verbose:
                print(clone_query)
            if not preview_only:
                try:
                    source_count = execute_count_query(f"select count(*) from {segment_table}")
                    if verbose:
                        print(f"source_count:{source_count}")
                    execute_single_query(clone_query, conn=conn, verbose=verbose)
                    cloned_count = execute_count_query(f"select count(*) from {cloned_table}")
                    if verbose:
                            print(f"cloned_count:{cloned_count}")
                    assert cloned_count == source_count, f"ERROR: expected cloned_count:{source_count} but got {cloned_count}"
                    newly_cloned_tables.append(info_schema_table_name)
                except Exception as e:
                    print(f"{type(e)} {str(e)}")
    if verbose:
        print("newly_cloned_tables:\n", newly_cloned_tables)
    return newly_cloned_tables


################################################
# Tests
################################################

def test_clone_latest_segment_tables_df():
    [csv_file,latest_df] = get_segment_tables_df(load_latest=True)
    clone_segment_tables(latest_df, preview_only=False)

def test_compute_and_save_new_segment_tables_df():
    csv, df = compute_and_save_new_segment_tables_df()
    
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

def tests():
    test_compute_and_save_new_segment_tables_df()
    test_find_segment_table_columns()
    # test_clone_latest_segment_tables_df()

    
    print()
    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()
    
if __name__ == "__main__":
    main()

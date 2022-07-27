import os
import sys
import pandas as pd
from requests import get
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from query_generator import query_batch_generator, create_connector, execute_batched_select_query, execute_single_query, execute_count_query, execute_simple_query
from utils import matches_any, find_latest_file, is_readable_file
from functools import cache
import datetime
from timefunc import timefunc
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
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

# Returns a list of metadata_tables that end with IDENTIFIES 
# and already exist in SEGMENT.IDENTIFIES_METADATA
def find_existing_metadata_tables(conn: connector=None, verbose:bool=True) -> List[str]:
    existing_metadata_tables = []
    show_tables_columns = ['created_on','name','database_name','schema_name	kind','comment','cluster_by	rows','bytes','owner','retention_time','automatic_clustering','change_tracking','search_optimization','search_optimization_progress','search_optimization_bytes','is_external']
    show_tables_query = "show tables like '%IDENTIFIES' in SEGMENT.IDENTIFIES_METADATA"
    results = execute_simple_query(show_tables_query, conn=conn)
    for line in results:
        line_dict = dict(zip(show_tables_columns, line))
        existing_metadata_tables.append(f"{line_dict['name']}")
    return existing_metadata_tables

# Creates a clone in SEGMENT.IDENTIFIES_METADATAfor for each 
# segment_table in SEGMENT described in segment_tables_df.
# Returns a list of the metadata_tables in SEGMENT.IDENTIFIES_METADATA 
# that have been newly cloned from source segment_tables in SEGMENT
def clone_segment_tables(segment_tables_df, conn: connector=None, verbose:bool=True, preview_only: bool=True) -> List[str]:
    existing_metadata_tables = find_existing_metadata_tables(conn=conn)
    newly_cloned_tables = []
    for row in segment_tables_df.values:
        segment_table = row[0]
        metadata_table = get_metadata_table_from_segment_table(segment_table)
        if metadata_table not in existing_metadata_tables:
            cloned_table = f"SEGMENT.IDENTIFIES_METADATA.{metadata_table}"
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
                    newly_cloned_tables.append(metadata_table)
                except Exception as e:
                    print(f"{type(e)} {str(e)}")
    if verbose:
        print("newly_cloned_tables:\n", newly_cloned_tables)
    return newly_cloned_tables

# Returns a list of segment_tables that have not been
# cloned into SEGMENT.IDENTIFIES_METADATA from SEGMENT
def find_uncloned_segment_tables(segment_tables_df, conn: connector=None, verbose:bool=True) -> List[str]:
    existing_metadata_tables = find_existing_metadata_tables(conn=conn)
    required_segment_tables = [row[0] for row in segment_tables_df.values]
    required_metadata_tables = [get_metadata_table_from_segment_table(x) for x in required_segment_tables ]
    uncloned_metadata_tables = list(set(required_metadata_tables) - set(existing_metadata_tables))
    uncloned_segment_tables = [get_segment_table_from_metadata_table(x) for x in uncloned_metadata_tables]
    return uncloned_segment_tables

################################################
# Tests
################################################

def test_clone_latest_segment_tables():
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    clone_segment_tables(latest_df, preview_only=False)

def test_find_uncloned_segment_tables():
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    uncloned_segment_tables = find_uncloned_segment_tables(latest_df)
    print(f"uncloned_segment_tables: {len(uncloned_segment_tables)}")
    for segment_table in uncloned_segment_tables:
        print(f"  uncloned segment_table: {segment_table}")

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
    test_clone_latest_segment_tables()
    test_find_uncloned_segment_tables()
    
    print()
    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()
    
if __name__ == "__main__":
    main()

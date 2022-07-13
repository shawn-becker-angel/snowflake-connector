import sys
import pandas as pd
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from query_generator import query_batch_generator, create_connector
from utils import is_empty_list, matches_any, find_latest_file, is_empty_df, is_readable_file
from functools import cache
import datetime
from timefunc import timefunc
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from key_column_infos import get_key_column_infos
from segment_utils import get_segment_table_from_info_schema_table_name


# Returns the set of all queryable segment_tables that have all key_columns
def compute_segment_tables(verbose: bool=True) -> Set[str]:
    # set of all filtered segment_tables that contain all key_columns
    segment_tables = set()
    
    dot_freq = 10000
    dot_char = '.'

    # the number of column names found in a given segment_table
    segment_table_column_count = {}
    
    # the number of times each column name is found among all segment_tables
    column_name_counts = {}
    
    # keeps the set of existing key_columns for a given segment_table
    # { segment_table:<segment_table>, key_columns_set: (<key_column>...) }
    segment_table_key_column_sets = {} 
    
    # skip info_schema_table_names that contain any of the following phrases 
    segment_table_filters = set(["DEV","STAGING"])
    
    query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
    query_batch_iterator = query_batch_generator(query, timeout_seconds=15)

    total_rows = 0
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
        
                # table__name: e.g. SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
                info_schema_table_name = result_row[0]
                # segment_table: e.g. SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
                segment_table = get_segment_table_from_info_schema_table_name(info_schema_table_name)

                segment_table_column_count[segment_table] = segment_table_column_count.get(segment_table, 0) + 1
                column_name = result_row[1]
                column_name_counts[column_name] = column_name_counts.get(column_name, 0) + 1
                
                action = 'o' # column skipped
                if column_name in ALL_KEY_COLUMNS_SET and not matches_any(segment_table, segment_table_filters):
                    key_columns_set = segment_table_key_column_sets.get(segment_table, set())
                    key_columns_set.add(column_name)
                    segment_table_key_column_sets[segment_table] = key_columns_set
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
    
    if verbose:
        print(f"\nfound {total_rows} columns")
        print(f"\nfound {len(segment_table_column_count)} tables")
    
    if verbose:
        print("\ntop 10 segment_table column counts")
        sorted_table_column_counts = sorted(segment_table_column_count.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_table_column_counts[i])
            
    if verbose:
        print("\ntop 10 column name counts")
        sorted_column_name_counts = sorted(column_name_counts.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_column_name_counts[i])
    
    if verbose:
        print("\nkey column counts:")
        for key in ALL_KEY_COLUMNS_SET:
            print(f"{key}: {column_name_counts.get(key)}")
    
    if verbose:
        print(f"\n{len(segment_table_key_column_sets.keys())} tables with any key_columns:", ALL_KEY_COLUMNS_SET)
    for segment_table, table_key_columns_set in segment_table_key_column_sets.items():
        if table_key_columns_set == ALL_KEY_COLUMNS_SET:
            assert segment_table not in segment_tables, f"ERROR: {segment_table} already saved"
            segment_tables.add(segment_table)
            if verbose:
                print(segment_table)
        
    return segment_tables

def save_segment_tables(segment_tables: Set[str]) -> Tuple[str, pd.DataFrame]:
    segment_tables_df = pd.DataFrame(data=segment_tables, columns=['segment_table'])
    utc_now = datetime.datetime.utcnow().isoformat()
    csv_file_path = f"/tmp/segment_tables-{utc_now}.csv"
    segment_tables_df.to_csv(csv_file_path)
    return (csv_file_path, segment_tables_df)

def get_segment_tables(verbose: bool=False) -> Tuple[str, pd.DataFrame]:
    segment_tables_df = None
    csv_file_path = None
    if USE_LATEST_SEGMENT_TABLES_CSV_FILE:
        csv_file_path = find_latest_file("/tmp/segment_tables-*.csv")
        if is_readable_file(csv_file_path):
            print("load segment_tables")
            segment_tables_df = pd.read_csv(csv_file_path)
    
    if is_empty_df(segment_tables_df):
        # compute and save
        print("compute segment_tables")
        segment_tables = compute_segment_tables(verbose=verbose)
        
        print("save segment_tables")
        (csv_file_path, segment_tables_df) = save_segment_tables(segment_tables)
        
    return (csv_file_path, segment_tables_df)
    


############# TESTS #################

def tests():
    print("all tests passed in", os.path.basename(__file__))

@timefunc
def main():
    tests()
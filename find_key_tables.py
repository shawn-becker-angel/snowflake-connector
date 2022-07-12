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
from query_generator import create_connector, query_row_processor

# more constants
ALL_KEY_COLUMNS_SET = set(["ANONYMOUS_ID", "USER_ID", "EMAIL"])
ALL_KEY_COLUMNS_LIST = sorted(list(ALL_KEY_COLUMNS_SET))
KEY_COLUMN_COUNTS_MIN_TIMESTAMP = "'2022-07-01'"
KEY_COLUMN_COUNTS_MAX_TIMESTAMP = "'2022-07-08'"
DEFAULT_FIRST_TIMESTAMP_COLUMN_AS_BEST = True
USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE = True

# Returns True if name contains any filter in filters
def matches_any(name, filters) -> bool:
    for filter in filters:
        if filter in name:
            return True
    return False

# Returns the set of all SEGMENT table_entries that have all key_columns
def get_SEGMENT_table_entries_with_all_key_columns(verbose: bool=True) -> Set[str]:
    # set of all filtered table_entries that contain all key_columns
    table_entries_with_all_key_columns = set()
    
    dot_freq = 10000
    dot_char = '.'

    # the number of column names found in a given table_entry
    table_entry_column_count = {}
    
    # the number of times each column name is found among all table_entries
    column_name_counts = {}
    
    # keeps the set of existing key_columns for a given table_entry
    # { table_entry:<table_entry>, key_columns_set: (<key_column>...) }
    table_entry_key_column_sets = {} 
    
    # skip table_names that contain any of the following phrases 
    table_entry_filters = set(["DEV","STAGING"])
    
    query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
    query_iterator = query_row_processor(query)

    total_rows = 0
    while True:
        try:
            result_row = next(query_iterator)
        
            # table__name: e.g. SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
            table_name = result_row[0]
            # table_entry: e.g. SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
            table_entry = get_table_entry_from_table_name(table_name)

            table_entry_column_count[table_entry] = table_entry_column_count.get(table_entry, 0) + 1
            column_name = result_row[1]
            column_name_counts[column_name] = column_name_counts.get(column_name, 0) + 1
            
            if column_name in ALL_KEY_COLUMNS_SET and not matches_any(table_entry, table_entry_filters):
                key_columns_set = table_entry_key_column_sets.get(table_entry, set())
                key_columns_set.add(column_name)
                table_entry_key_column_sets[table_entry] = key_columns_set
              
            if verbose:      
                print(result_row)  
            else: 
                if total_rows % dot_freq == 0:
                    sys.stdout.write(dot_char)
                    sys.stdout.flush()
 
            total_rows += 1                
        
        except StopIteration:
            break
    
    if verbose:
        print(f"\nfound {total_rows} columns")
        print(f"\nfound {len(table_entry_column_count)} tables")
    
    if verbose:
        print("\ntop 10 table_entry column counts")
        sorted_table_column_counts = sorted(table_entry_column_count.items(), key=lambda e: e[1], reverse=True)
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
        print(f"\n{len(table_entry_key_column_sets.keys())} tables with any key_columns:", ALL_KEY_COLUMNS_SET)
    for table_entry, table_key_columns_set in table_entry_key_column_sets.items():
        if table_key_columns_set == ALL_KEY_COLUMNS_SET:
            assert table_entry not in table_entries_with_all_key_columns, f"ERROR: {table_entry} already saved"
            table_entries_with_all_key_columns.add(table_entry)
            if verbose:
                print(table_entry)
    
    return table_entries_with_all_key_columns


# Returns a dict of key_column_names and key_column_counts
#    key_column_counts: {
#     'ANONYMOUS_ID': 46, 
#     'EMAIL': 32, 
#     'USER_ID': 32
#    }
def parse_key_column_counts(result_row_parts: List[str]) -> Dict[str,str]:
    key_column_names = list(ALL_KEY_COLUMNS_SET)
    key_column_counts = result_row_parts
    assert len(key_column_names) == len(key_column_counts), "ERROR: in lengths of keys and values"
    return dict(zip(key_column_names, key_column_counts))

# Returns the query template used by get_table_entry_key_column_counts_query()
@cache
def get_table_entry_key_column_counts_query_template():
    select_clause = ",".join([f"count(distinct({x}))" for x in ALL_KEY_COLUMNS_LIST])
    not_null_clause = " and ".join([f"{x} is not NULL" for x in ALL_KEY_COLUMNS_LIST])
    query_template = f"SELECT {select_clause} FROM <<table_entry>> WHERE {not_null_clause}"
    return query_template

# Return the query to get counts of all key_columns in a table_entry. Uses the
# optional timestamp_column to reduce query processing time and result size
def get_table_entry_key_column_counts_query(table_entry, timestamp_column: Optional[str]=None) -> str:
    template = get_table_entry_key_column_counts_query_template()
    query = template.replace("<<table_entry>>",table_entry)
    if timestamp_column is not None:
        when_clause = f" and {timestamp_column} between TO_TIMESTAMP_NTZ({KEY_COLUMN_COUNTS_MIN_TIMESTAMP}) and TO_TIMESTAMP_NTZ({KEY_COLUMN_COUNTS_MAX_TIMESTAMP})"
        query  += when_clause
    return query

# attempt to find the timestamp_column for the given table_entry that has the lowest min_timestamp
def find_table_entry_best_timestamp_column(table_entry: str, min_allowed_timestamp: float=None, verbose: bool=False, conn: connector=None):
    best_timestamp_column = None
    lowest_min_timestamp = None
    table_entry_timestamp_columns = find_table_entry_timestamp_columns(table_entry, conn=conn)
    for timestamp_column in table_entry_timestamp_columns:
        
        # datetime_range will be (None,None) if this query times out
        timestamp_range = find_timestamp_column_range(table_entry, timestamp_column, conn=conn)
        if timestamp_range[0] is not None:
            min_timestamp = math.max(timestamp_range[0], min_allowed_timestamp)
            if verbose:
                print(f"table:{table_entry} column:{timestamp_column} timestamp_range:{timestamp_range}")
            if lowest_min_timestamp is None or timestamp_range[0] < lowest_min_timestamp:
                    lowest_min_timestamp = timestamp_range[0]
                    best_timestamp_column = timestamp_column
    
    # use first as default best_timestamp_column 
    if DEFAULT_FIRST_TIMESTAMP_COLUMN_AS_BEST:
        if best_timestamp_column is None and len(table_entry_timestamp_columns) > 0:
            best_timestamp_column = table_entry_timestamp_columns[0]
    
    return best_timestamp_column

    
# Returns list of utc timestamped key_column_counts for all SEGMENT_table_entries.
# Run and log the results of this task periodically to compare growth rates of key_column counts in tables over time.
# 
# Structure:
# table_entries_key_column_counts = [ 
#  { 
#    datetime: 2022-07-07T21:43:16.011804, 
#    table_entry: SEGMENT__THE_CHOSEN_APP_WEB_PROD__IDENTIFIES, 
#    key_column_counts: {
#     'ANONYMOUS_ID': 46, 
#     'EMAIL': 32, 
#     'USER_ID': 32
#    }
#  },
# ...
# ]
@timefunc
def get_SEGMENT_table_entries_key_column_counts(table_entries_with_all_key_columns: Set[str], verbose: bool=True) -> List[Dict]:

    table_entries_key_column_counts = []
    sorted_table_entries = sorted(list(table_entries_with_all_key_columns))
    uncounted_table_entries = set(sorted_table_entries)
    counted_table_entries = set()
    
    conn = create_connector()
    print(f"\nnew connector: {WAREHOUSE} {DATABASE} {SCHEMA}")
        
    for table_entry in sorted_table_entries: 
        
        # try to find the best column to use for timestamp filtering this table entry
        min_allowed_timestamp = datetime.datetime(2022,1,1).timestamp()
        print(f"find_table_entry_best_timestamp_column() table_entry:{table_entry}")
        best_timestamp_column = find_table_entry_best_timestamp_column(table_entry, min_allowed_timestamp=min_allowed_timestamp, verbose=verbose, conn=conn)
        
        # use the best_timestamp_column if defined
        query = get_table_entry_key_column_counts_query(table_entry, timestamp_column=best_timestamp_column) 
        
        try:
            batch_size = 10
            timeout_seconds = 10
            cur = conn.cursor()
            
            # NOTE: have seen 4 timeouts out of 76 tables
            cur.execute(query, timeout=timeout_seconds)

            num_batches = 0
            total_rows = 0
            while True:
                try:
                    result_rows = cur.fetchmany(batch_size) 
                    num_result_rows = len(result_rows)
                    if num_result_rows == 0:
                        break

                    for result_row_parts in result_rows:
                        key_column_counts =  parse_key_column_counts(result_row_parts)
                        table_entry_key_column_counts = {
                            "datetime": datetime.datetime.utcnow().isoformat(),
                            "table_entry": table_entry,
                            "key_column_counts": key_column_counts
                        }
                        table_entries_key_column_counts.append(table_entry_key_column_counts)
                        if verbose:
                            print(table_entry_key_column_counts)

                    total_rows += num_result_rows
                    num_batches += 1
                    
                except StopIteration:
                    break

            # table_entry successfully counted
            counted_table_entries.add(table_entry)
            uncounted_table_entries.remove(table_entry)
            
        except ProgrammingError as err:
            if err.errno == 604:
                print(timeout_seconds, "second timeout for query:\n", query)
            else:
                print(f"Error: {type(err)} {str(err)}")
        finally:
            cur.close()
    # end for table_entry
    
    ### Extra verbosity
    if verbose:
        # display query strings for all skipped table_entries
        print("\nquery strings for", len(uncounted_table_entries), "skipped table_entries out of", len(sorted_table_entries) )
        for table_entry in sorted(uncounted_table_entries):
            query_string = get_table_entry_key_column_counts_query(table_entry)
            print(query_string + ';')

        # display passed table_entries
        print("\npassed", len(counted_table_entries), "tables out of", len(sorted_table_entries) )
        for counted_table_entry in sorted(counted_table_entries):
            print("passed", counted_table_entry)

    assert len(table_entries_key_column_counts) == len(counted_table_entries), "ERROR: count failure"
    
    conn.close()
    
    return table_entries_key_column_counts

# Returns csv file path and pd.DataFrame of the given list of dicts
def create_TABLE_ENTRIES_COLUMN_COUNTS_csv_file(table_entries_key_column_counts: List[Dict]) -> Tuple[str, pd.DataFrame]:
    utc_now = datetime.datetime.utcnow().isoformat()
    csv_file_path = f"/tmp/table_entries-column-counts-{utc_now}.csv"
    df = pd.json_normalize(table_entries_key_column_counts)
    df.to_csv(csv_file_path)
    return (csv_file_path, df)

# Returns table_name: SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
# given table_entry: SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
def get_table_name_from_table_entry(table_entry):
    # handle special case #111
    table_entry = table_entry.replace("LOOKER_SOURCE.PUBLIC.","")
    return table_entry.replace(".","__")

# Returns table_entry: SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
# given table_name: SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
def get_table_entry_from_table_name(table_name):
    table_entry = table_name.replace("__",".")
    parts = table_entry.split(".")

    # special case #111: 
    # set database and schema prefix for table_entry when only table part is available.
    # e.g. like 'ANGL_APP_OPN_TO_PIF'
    if len(parts) == 1:
        table_entry = 'LOOKER_SOURCE.PUBLIC.' + table_entry
        
    return table_entry

# Return a list of all column_names in table_entry with TIMESTAMP datatypes
def find_table_entry_timestamp_columns(table_entry, conn: connector):
    table_name = get_table_name_from_table_entry(table_entry)
    timestamp_columns = []
    
    # NOTE that DATETIME is an alias for TIMESTAMP_NTZ
    # see https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
    query = f"SELECT COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{table_name}' and DATA_TYPE = 'TIMESTAMP_NTZ'"
    query_iterator = query_row_processor(query, conn=conn)
    while True:
        try:
            result_row = next(query_iterator)
            timestamp_columns.append(result_row[0])
        except StopIteration:
            break

    return timestamp_columns

# Return the min and max timestamps for the given table_entry and timestamp_column_name
def find_timestamp_column_range(table_entry, timestamp_column_name, conn: connector):
    min_timestamp = max_timestamp = None
    query = f"SELECT min({timestamp_column_name}), max({timestamp_column_name}) FROM {table_entry}"
    query_iterator = query_row_processor(query, conn=conn)
    while True:
        try:
            # NOTE that DATETIME is an alias for TIMESTAMP_NTZ
            # see https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
            min_dt, max_dt = next(query_iterator)
            assert isinstance(min_dt, datetime.datetime)
            assert isinstance(min_dt, datetime.datetime)
            min_timestamp = min_dt.timestamp()
            max_timestamp = max_dt.timestamp()
            return min_timestamp, max_timestamp
        except StopIteration:
            break
    return min_timestamp, max_timestamp

# Returns a csv file path and a DataFrame of ellis_island_users with 
# columns ["uuid","username","inserted_at","updated_at","email"]
def create_ELLIS_ISLAND_USERS_csv_file() -> Tuple[str, pd.DataFrame]:
    ellis_island_users = []
    columns = ["uuid","username","inserted_at","updated_at","email"]
    dot_freq = 10000
    dot_char = '.'
    query = \
"""select u.uuid, u.username, u.inserted_at, u.updated_at, a.data:email 
    FROM STITCH_LANDING.ELLIS_ISLAND.USER u 
    join STITCH_LANDING.ELLIS_ISLAND.SOCIAL_AUTH a 
    on a.user_id = u.id
"""
    query_iterator = query_row_processor(query)
    num_users = 0
    print()
    while True:
        try:
            result_row = next(query_iterator)
            if len(result_row) == 5:
                ellis_island_user = dict(zip(columns, result_row))
                ellis_island_users.append(ellis_island_user)
                
                if num_users % dot_freq == 0:
                    sys.stdout.write(dot_char)
                    sys.stdout.flush()
                num_users += 1

            else:
                print("skipping invalid result_row:", result_row)
        except StopIteration:
            break
    print()
    ellis_island_users_df = pd.DataFrame(data=ellis_island_users, columns=columns)
    utc_now = datetime.datetime.utcnow().isoformat()
    csv_file_path = f"/tmp/ellis_island_users-{utc_now}.csv"
    ellis_island_users_df.to_csv(csv_file_path)
    return (csv_file_path, ellis_island_users_df)

# Returns the latest file that matches the given pattern
# or None if no such file found.
# For example: pattern="/tmp/ellis_island_users-*.csv"
# returns the most recent ellis_island_users csv file under /tmp
def find_latest_file(pattern):
    # get list of files that matches pattern
    files = list(filter(os.path.isfile, glob.glob(pattern)))
    if files is not None and len(files) > 0:
        # sort by modified time
        files.sort(key=lambda x: os.path.getmtime(x))
        # return last item in list
        return files[-1]
    return None

# Returns an ELLIS_ISLAND_USERS csv file and DataFrame
# either by loading the latest or by creating a new one.
def get_ELLIS_ISLAND_USERS_csv_file() -> Tuple[str, pd.DataFrame]:
    df = None
    if USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE:
        csv_file = find_latest_file(pattern="/tmp/ellis_island_users-*.csv")
        if csv_file is not None and os.path.isfile(csv_file):
            df = pd.read_csv(csv_file)
            print(f"loaded csv_file {csv_file} with {len(df)} users")
    if df is None:
        csv_file, df = create_ELLIS_ISLAND_USERS_csv_file()
        print(f"created csv_file {csv_file} with {len(df)} users")
    return csv_file, df

# Returns new augmented_table_entry_users_df with columns ['USER_ID', 'EMAIL', 'UUID'] by
# doing a left outer join with ellis_island_users_df
# NOTE that the new 'UUID' column may be None
def find_augmented_SEGMENT_table_entry_users_df(table_entry_with_key_columns: str, ellis_island_users_df):
    query = f"SELECT USER_ID, EMAIL from {table_entry_with_key_columns}"
    segment_columns = ["USER_ID", "EMAIL"]
    users_df = ellis_island_users_df
    where_clause = "(segment_df['USER_ID'] = users_df['UUID']) or (segment_df['EMAIL'] = user_df['EMAIL']) or (segment_df['EMAIL'] = user_df['USERNAME'])"

    augmented_table_entry_users_df = None
    query_iterator = query_row_processor(query)
    while True:
        try:
            segment_rows = next(query_iterator)
            num_rows = len(segment_rows)
            if num_rows == 0:
                break
            segment_df = pd.DataFrame(data=segment_rows, columns=segment_columns)
            joined_df = pd.merge(left=segment_df, right=users_df, how="left", where=where_clause)
            segment_df = pd.concat([segment_df, joined_df['UUID']], axis=1)
            augmented_table_entry_users_df = segment_df if augmented_table_entry_users_df.empty else pd.concat([augmented_table_entry_users_df, segment_df], axis=0)

        except StopIteration:
            break
    
    return augmented_table_entry_users_df

# Print stats of # rows with or withou valid UUID for each SEGMENT table_entry with all key_columns
@timefunc
def find_augmented_SEGMENT_table_entries_users(table_entries_with_all_key_columns: List[str]):
    _, users_df = get_ELLIS_ISLAND_USERS_csv_file()
    for table_entry in table_entries_with_all_key_columns:
        augmented_table_entry_users_df = find_augmented_SEGMENT_table_entry_users_df(table_entry, users_df)
        num_null_UUID_rows = sum([True for idx,row in augmented_table_entry_users_df.iterrows() if row['UUID'].isnull()])
        num_valid_UUID_rows = sum([True for idx,row in augmented_table_entry_users_df.iterrows() if ~row['UUID'].isnull()])
        assert num_null_UUID_rows + num_valid_UUID_rows == len(augmented_table_entry_users_df), f"ERROR: row count failure for table_entry:{table_entry}"
        print("table_entry: {table_entry} num_null_UUID_rows: {num_null_UUID_rows} num_valid_UUID_rows:{num_valid_UUID_rows}")

    
############# TESTS #################

@timefunc
def test_get_table_entry_key_column_counts_query() -> bool:
    table_entry = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_GNRL'
    expected = "SELECT count(distinct(ANONYMOUS_ID)),count(distinct(EMAIL)),count(distinct(USER_ID)) FROM LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_GNRL WHERE ANONYMOUS_ID is not NULL and EMAIL is not NULL and USER_ID is not NULL"
    result = get_table_entry_key_column_counts_query(table_entry)
    assert result == expected, f"ERROR: expected:\n{expected}\nnot result:\n{result}"
    
    table_entry = 'SEGMENT.ANGEL_MOBILE_ANDROID_PROD.USER_SIGN_IN_STARTED'
    expected = "SELECT count(distinct(ANONYMOUS_ID)),count(distinct(EMAIL)),count(distinct(USER_ID)) FROM SEGMENT.ANGEL_MOBILE_ANDROID_PROD.USER_SIGN_IN_STARTED WHERE ANONYMOUS_ID is not NULL and EMAIL is not NULL and USER_ID is not NULL"
    result = get_table_entry_key_column_counts_query(table_entry)
    assert result == expected, f"ERROR: expected:\n{expected} not result:\n{result}"

    print("all tests passed")
    return True      

# Returns True if all tests passed
def passed_all_tests() -> bool:
    num_failed_tests = 0
    if test_get_table_entry_key_column_counts_query() is not True:
        num_failed_tests += 1
    return True if num_failed_tests == 0 else False

@timefunc
def main():
    assert passed_all_tests() is True, "ERROR: failed some tests"
    
    csv_file, df = get_ELLIS_ISLAND_USERS_csv_file()
    
    # create a list of all SEGMENT table_entries with all key columns
    print("get_SEGMENT_table_entries_with_all_key_columns")
    table_entries_with_all_key_columns = get_SEGMENT_table_entries_with_all_key_columns(verbose=False)
    print("\nnum SEGMENT table_entries_with_all_key_columns:", len(table_entries_with_all_key_columns))
    
    # save the current key_column_counts over a fixed period of time for all SEGMENT table_entries that have all key columns
    print("get_SEGMENT_table_entries_key_column_counts")
    table_entries_key_column_counts = get_SEGMENT_table_entries_key_column_counts(table_entries_with_all_key_columns)
    csv_file, df = create_TABLE_ENTRIES_COLUMN_COUNTS_csv_file(table_entries_key_column_counts)
    print(f"created csv_file {csv_file} with {len(df)} users")

    # compute the current number of rows with matching UUIDs over a fixed period of time for all table_entries that have all key columns
    print("find_augmented_SEGMENT_table_entries_users")
    find_augmented_SEGMENT_table_entries_users(table_entries_with_all_key_columns)

if __name__ == "__main__":
    
    main()
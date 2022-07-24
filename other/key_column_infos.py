import sys
import pandas as pd
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from query_generator import query_batch_generator, create_connector
from utils import is_empty_list, find_latest_file, is_readable_file
from data_frame_utils import is_empty_data_frame
from functools import cache
import datetime
from timefunc import timefunc
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from segment_utils import get_info_schema_table_name_from_segment_table


# Returns the query template used by get_key_column_info_query_info()
@cache
def get_key_column_info_query_template():
    select_clause = ",".join([f"count(distinct({x}))" for x in ALL_KEY_COLUMNS])
    not_null_clause = " and ".join([f"{x} is not NULL" for x in ALL_KEY_COLUMNS])
    query_template = f"SELECT {select_clause} FROM <<segment_table>> WHERE {not_null_clause}"
    return query_template

# Compute the query to get counts of all key_columns in a segment_table. Uses the
# optional timestamp_column to reduce query processing time and result size.
# Return query_info dict with query and optional imestamp_column, min_datetime_filter, and max_datetime_filter
def get_key_column_info_query_info(segment_table, timestamp_column: Optional[str]=None) -> Dict:
    template = get_key_column_info_query_template()
    query = template.replace("<<segment_table>>",segment_table)
    min_datetime_filter = max_datetime_filter = None
    if timestamp_column is not None:
        min_datetime_filter = KEY_COLUMN_COUNTS_MIN_TIMESTAMP
        max_datetime_filter = KEY_COLUMN_COUNTS_MAX_TIMESTAMP
        when_clause = f" and {timestamp_column} between TO_TIMESTAMP_NTZ({KEY_COLUMN_COUNTS_MIN_TIMESTAMP}) and TO_TIMESTAMP_NTZ({KEY_COLUMN_COUNTS_MAX_TIMESTAMP})"
        query  += when_clause
    query_info = {
        "query": query, 
        "timestamp_column": timestamp_column, 
        "min_datetime_filter": min_datetime_filter, 
        "max_datetime_filter": max_datetime_filter
    }
    return query_info

# Return a sorted list of all column_names in segment_table with TIMESTAMP datatypes
def find_sorted_timestamp_columns(segment_table, conn: connector):
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    timestamp_columns = []
    
    # NOTE that DATETIME is an alias for TIMESTAMP_NTZ
    # see https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
    query = f"SELECT COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{info_schema_table_name}' and DATA_TYPE = 'TIMESTAMP_NTZ'"
    query_batch_iterator = query_batch_generator(query, conn=conn)
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
                timestamp_columns.append(result_row[0])
        except StopIteration:
            break

    return sorted(timestamp_columns)


# Returns the first sorted timestamp_column for the given segment_table or None if segment_table was not queryable
def find_first_sorted_timestamp_column(segment_table: str, conn: connector=None):
    sorted_timestamp_columns = find_sorted_timestamp_columns(segment_table, conn=conn)
    return None if is_empty_list(sorted_timestamp_columns) else sorted_timestamp_columns[0]

# Returns a dict of key_column_names and key_column_counts
#    key_column_counts: {
#     'ANONYMOUS_ID': 46, 
#     'EMAIL': 32, 
#     'USER_ID': 32
#    }
def parse_key_column_counts(result_row_parts: List[str]) -> Dict[str,str]:
    key_column_names = list(ALL_KEY_COLUMNS)
    key_column_counts = result_row_parts
    assert len(key_column_names) == len(key_column_counts), "ERROR: in lengths of keys and values"
    return dict(zip(key_column_names, key_column_counts))

# Returns list of utc timestamped key_column_counts for all segment_tables.
# Run and log the results of this task periodically to compare growth rates of key_column counts in tables over time.
# 
# Structure:
# key_column_infos = [ 
#  { 
#    datetime: 2022-07-07T21:43:16.011804, 
#    segment_table: SEGMENT__THE_CHOSEN_APP_WEB_PROD__IDENTIFIES, 
#    key_column_counts: {
#     'ANONYMOUS_ID': 46, 
#     'EMAIL': 32, 
#     'USER_ID': 32
#    }
#  },
# ...
# ]
@timefunc
def compute_key_column_infos(segment_tables: Set[str], verbose: bool=True) -> List[Dict]:

    key_column_infos = []
    sorted_segment_tables = sorted(list(segment_tables))
    uncounted_segment_tables = set(sorted_segment_tables)
    counted_segment_tables = set()
    
    conn = None
    try:
        # connector for all segment_table queries
        conn = create_connector()
        for segment_table in sorted_segment_tables:
            cur = None
            try:
                # find the timestamp column to use for timestamp filtering this segment_table (or None)
                timestamp_column = find_first_sorted_timestamp_column(segment_table, conn=conn)
                query_info = get_key_column_info_query_info(segment_table, timestamp_column=timestamp_column) 
                query = query_info['query']
                cur = conn.cursor()
                cur.execute(query, timeout=DEFAULT_TIMEOUT_SECONDS)
                result_row = cur.fetchone()
                key_column_counts =  parse_key_column_counts(result_row)
                key_column_info = {
                    "datetime": datetime.datetime.utcnow().isoformat(),
                    "segment_table": segment_table,
                    "query_info": query_info,
                    "key_column_counts": key_column_counts
                }
                key_column_infos.append(key_column_info)
                if verbose:
                    print(segment_table, ":", key_column_counts)
                else:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    
                # mark segment_table as counted if query did not timeout or fail
                counted_segment_tables.add(segment_table)
                uncounted_segment_tables.remove(segment_table)
            
            # query timed out or failed
            except ProgrammingError as err:
                if err.errno == 604:
                    print(DEFAULT_TIMEOUT_SECONDS, "second timeout for query:\n", query)
                else:
                    print(f"Error: {type(err)} {str(err)}")
            finally:
                if cur is not None:
                    cur.close()
                    cur = None
                    
        # end for segment_tables ...
    finally:
        if conn is not None:
            conn.close()
            conn = None

    ### Extra verbosity
    if verbose:
        # display query strings for all skipped segment_tables
        print("\nquery strings for", len(uncounted_segment_tables), "skipped segment_tables out of", len(sorted_segment_tables) )
        for segment_table in sorted(uncounted_segment_tables):
            query_info = get_key_column_info_query_info(segment_table)
            print(query_info['query'] + ';')

        # display passed segment_tables
        print("\npassed", len(counted_segment_tables), "tables out of", len(sorted_segment_tables) )
        for counted_segment_table in sorted(counted_segment_tables):
            print("passed", counted_segment_table)

    assert (len(counted_segment_tables) + len(uncounted_segment_tables)) == len(segment_tables), "ERROR: count failure"
    assert len(key_column_infos) == len(counted_segment_tables), "ERROR: count failure"
    
    return key_column_infos

# Returns csv file path and pd.DataFrame of the given list of dicts
def save_key_column_infos(key_column_infos: List[Dict]) -> Tuple[str, pd.DataFrame]:
    utc_now = datetime.datetime.utcnow().isoformat()
    csv_file_path = f"/tmp/key_column_infos-{utc_now}.csv"
    df = pd.json_normalize(key_column_infos)
    df.to_csv(csv_file_path)
    return (csv_file_path, df)

# Returns the csv_file_path and DataFrame of the set of segment_tables_key_columns_info 
# If specified in constants.USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE
# attempts to load the DataFrame from the latest csv file.
# Otherwise, compute and save the key_column_infos from the set of segment_tables.
def get_key_column_infos(segment_tables: Set[str], verbose: bool=True) -> Tuple[str, pd.DataFrame]:
    key_column_infos_df = None
    key_column_infos_csv_path = None
    
    if USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE:
        key_column_infos_csv_path = find_latest_file("/tmp/key_column_infos-*.csv")
        if is_readable_file(key_column_infos_csv_path):
            print("load key_column_infos")
            df = pd.read_csv(key_column_infos_csv_path)
            if 0 < len(df) < len(segment_tables):
                key_column_infos_df = df
    
    if key_column_infos_df is None:
        print("compute key_column_infos")
        key_column_infos = compute_key_column_infos(segment_tables, verbose=False)
        
        print("save key_column_infos")
        (key_column_infos_csv_path, df) = save_key_column_infos(key_column_infos)
        if is_empty_data_frame(df) is False:
            key_column_infos_df = df
    
    return (key_column_infos_csv_path, key_column_infos_df)
    

############# TESTS #################

@timefunc
def test_get_key_column_info_query_info() -> bool:
    segment_table = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_GNRL'
    expected = "SELECT count(distinct(ANONYMOUS_ID)),count(distinct(EMAIL)),count(distinct(USER_ID)) FROM LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_GNRL WHERE ANONYMOUS_ID is not NULL and EMAIL is not NULL and USER_ID is not NULL"
    result = get_key_column_info_query_info(segment_table)['query']
    assert result == expected, f"ERROR: expected:\n{expected}\nnot result:\n{result}"
    
    segment_table = 'SEGMENT.ANGEL_MOBILE_ANDROID_PROD.USER_SIGN_IN_STARTED'
    expected = "SELECT count(distinct(ANONYMOUS_ID)),count(distinct(EMAIL)),count(distinct(USER_ID)) FROM SEGMENT.ANGEL_MOBILE_ANDROID_PROD.USER_SIGN_IN_STARTED WHERE ANONYMOUS_ID is not NULL and EMAIL is not NULL and USER_ID is not NULL"
    result = get_key_column_info_query_info(segment_table)['query']
    assert result == expected, f"ERROR: expected:\n{expected} not result:\n{result}"

    print("all tests passed")
    return True      

def tests():
    test_get_key_column_info_query_info()
    print("all tests passed in", os.path.basename(__file__))

@timefunc
def main():
    tests()
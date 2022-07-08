import sys
# import pandas as pd
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import Set, List, Dict
import datetime
from functools import cache

all_key_columns_set = set(["ANONYMOUS_ID", "USER_ID", "EMAIL"])
all_key_columns_list = sorted(list(all_key_columns_set))

# Returns True if name contains any filter in filters
def matches_any(name, filters) -> bool:
    for filter in filters:
        if filter in name:
            return True
    return False

# Returns a list of all filtered SEGMENT table_entries that contain all key_columns
@timefunc
def get_SEGMENT_table_entries_with_all_key_columns() -> Set[str]:
    # set of all filtered table_entries that contain all key_columns
    table_entries_with_all_key_columns = set()

    # the number of column names found in a given table_entry
    table_entry_column_count = {}
    
    # the number of times each column name is found among all table_entries
    column_name_counts = {}
    
    # keeps the set of existing key_columns for a given table_entry
    # { table_entry:<table_entry>, key_columns_set: (<key_column>...) }
    table_entry_key_column_sets = {} 
    
    # skip table_names that contain any of the following phrases 
    table_entry_filters = set(["DEV","STAGING"])
    
    batch_size = 10
    try:
        conn = connector.connect(
            user=USER_NAME,
            password=USER_PSWD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA,
            protocol='https',
            port=PORT)
        
        print(f"\nnew connection: {WAREHOUSE} {DATABASE} {SCHEMA}")

        cur = conn.cursor()
        query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
        cur.execute(query)
        
        num_batches = 0
        total_rows = 0
        while True:
            result_rows = cur.fetchmany(batch_size)
            num_result_rows = len(result_rows)
            if num_result_rows == 0:
                break
            for result_row in result_rows:
                
                # table__name: SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
                table__name = result_row[0]
                
                # table_entry: SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
                table_entry = table__name.replace("__",".")
                parts = table_entry.split(".")
        
                # set database and schema for table_entry that has only the table part, like 'ANGL_APP_OPN_TO_PIF'
                if len(parts) == 1:
                    table_entry = 'LOOKER_SOURCE.PUBLIC.' + table_entry
                
                table_entry_column_count[table_entry] = table_entry_column_count.get(table_entry, 0) + 1
                column_name = result_row[1]
                column_name_counts[column_name] = column_name_counts.get(column_name, 0) + 1
                
                if column_name in all_key_columns_set and not matches_any(table_entry, table_entry_filters):
                    key_columns_set = table_entry_key_column_sets.get(table_entry, set())
                    key_columns_set.add(column_name)
                    table_entry_key_column_sets[table_entry] = key_columns_set
                        
                print(result_row)
            total_rows += num_result_rows
            num_batches += 1
        
        print(f"\nfetched {total_rows} total_rows in {num_batches} batches")
        
        print(f"\nfound {total_rows} columns")
        print(f"\nfound {len(table_entry_column_count)} tables")
        
        print("\ntop 10 table_entry column counts")
        sorted_table_column_counts = sorted(table_entry_column_count.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_table_column_counts[i])

        print("\ntop 10 column name counts")
        sorted_column_name_counts = sorted(column_name_counts.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_column_name_counts[i])
            
        print("\nkey column counts:")
        for key in all_key_columns_set:
            print(f"{key}: {column_name_counts.get(key)}")
        
        print(f"\n{len(table_entry_key_column_sets.keys())} tables with any key_columns:", all_key_columns_set)
        for table_entry, table_key_columns_set in table_entry_key_column_sets.items():
            if table_key_columns_set == all_key_columns_set:
                assert table_entry not in table_entries_with_all_key_columns, f"ERROR: {table_entry} already saved"
                table_entries_with_all_key_columns.add(table_entry)
                print(table_entry)
        
        return table_entries_with_all_key_columns
        
    except ProgrammingError as err:
        if err.errno == 604:
            print("timeout")
        else:
            print(f"Error: {type(err)} {str(err)}")

    finally:
        cur.close()
        conn.close()

# create a dict from key_column_names and key_column_counts
def parse_key_column_counts(result_row_parts: List[str]) -> Dict[str,str]:
    key_column_names = list(all_key_columns_set)
    key_column_counts = result_row_parts
    assert len(key_column_names) == len(key_column_counts), "ERROR: in lengths of keys and values"
    return dict(zip(key_column_names, key_column_counts))

@cache
def get_table_entry_key_column_counts_query_template():
    select_clause = ",".join([f"count(distinct({x}))" for x in all_key_columns_list])
    where_clause = " and ".join([f"{x} is not NULL" for x in all_key_columns_list])
    query_template = f"SELECT {select_clause} FROM <<table_entry>> WHERE {where_clause}"
    return query_template

def get_table_entry_key_column_counts_query(table_entry):
    template = get_table_entry_key_column_counts_query_template()
    return template.replace("<<table_entry>>",table_entry)

# Return list of utc timestamped key_column_counts for all SEGMENT_table_entries.
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
def get_SEGMENT_table_entries_key_column_counts(table_entries_with_all_key_columns: Set[str]) -> List[Dict]:
    table_entries_key_column_counts = []
    
    sorted_table_entries = sorted(list(table_entries_with_all_key_columns))
    uncounted_table_entries = set(sorted_table_entries)
    counted_table_entries = set()
        
    conn = connector.connect(
        user=USER_NAME,
        password=USER_PSWD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        protocol='https',
        port=PORT)
    print(f"\nnew connection: {WAREHOUSE} {DATABASE} {SCHEMA}")
        
    for table_entry in sorted_table_entries:  
        query = get_table_entry_key_column_counts_query(table_entry) 
        try:
            batch_size = 10
            timeout_seconds = 10
            
            cur = conn.cursor()
            cur.execute(query, timeout=timeout_seconds)

            num_batches = 0
            total_rows = 0
            while True:
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
                    print(table_entry_key_column_counts)

                total_rows += num_result_rows
                num_batches += 1

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
    
    # display query strings for all skipped table_entries
    print("\nquery strings for", len(uncounted_table_entries), "skipped table_entries out of", len(sorted_table_entries) )
    for table_entry in sorted(uncounted_table_entries):
        query_string = get_table_entry_key_column_counts_query(table_entry)
        print(query_string + ';')

    print("\npassed", len(counted_table_entries), "tables out of", len(sorted_table_entries) )
    for counted_table_entry in sorted(counted_table_entries):
        print("passed", counted_table_entry)

    assert len(table_entries_key_column_counts) == len(counted_table_entries), "ERROR: count failure"
    
    conn.close()
    
    return table_entries_key_column_counts


# create a timestamped list of all unique collections of key_column values from a given table_entry. For example:
# table_entry_key_column_values = [
#   {
#     'datetime': '2022-07-07T22:11:39.989134', 
#     'table_entry': 'SEGMENT__THE_CHOSEN_MOBILE_IOS_PROD__IDENTIFIES', 
#     'ANONYMOUS_ID': '60238a52-69d7-4ca2-8659-506c8214bdce', 
#     'USER_ID': 'ac1af734-d5fb-49b1-b649-cfa640b3ba27',
#     'EMAIL': 'howdy@doodie.com'
#   },
#   ...
# ]
@timefunc
def get_SEGMENT_key_column_values(table_entries_key_column_counts):
    key_column_values = []
    return key_column_values

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


@timefunc
def main():
    assert test_get_table_entry_key_column_counts_query() == True, "ERROR: all tests did not pass"
    
    table_entries_with_all_key_columns = get_SEGMENT_table_entries_with_all_key_columns()
    print("\nnum SEGMENT table_entries_with_all_key_columns:", len(table_entries_with_all_key_columns))
    
    table_entries_key_column_counts = get_SEGMENT_table_entries_key_column_counts(table_entries_with_all_key_columns)
    print("\nnum SEGMENT table_entries_key_column_counts:", len(table_entries_key_column_counts))

    key_column_values = get_SEGMENT_key_column_values(table_entries_key_column_counts)
    print("\nnum SEGMENT key_column_values:", len(key_column_values))

if __name__ == "__main__":
    
    main()
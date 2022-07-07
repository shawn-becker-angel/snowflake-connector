import sys
# import pandas as pd
import snowflake.connector as connector
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import Set, List, Dict
import datetime

all_key_columns_set = set(["ANONYMOUS_ID", "USER_ID", "EMAIL"])

# Returns True if name contains any filter in filters
def matches_any(name, filters) -> bool:
    for filter in filters:
        if filter in name:
            return True
    return False

# Returns a list of all filtered table_column_count that contain all key_columns
@timefunc
def find_tables_with_key_columns() -> Set[str]:
    
    # the number of column names found in a given table
    table_column_count = {}
    
    # the number of times each column name is found among all tables
    column_name_counts = {}
    
    # keeps the set of existing key_columns for a given table
    # { table_name:<table_name>, key_columns_set: (<key_column>...) }
    table_key_columns_sets = {} 
    
    # skip table_names that contain any of the following phrases 
    table_name_filters = set(["DEV","STAGING"])
    
    warehouse = WAREHOUSE
    database = DATABASE
    schema = 'INFORMATION_SCHEMA'
    batch_size = 10
    try:
        conn = connector.connect(
            user=USER_NAME,
            password=USER_PSWD,
            account=ACCOUNT,
            warehouse=warehouse,
            database=database,
            schema=schema,
            protocol='https',
            port=PORT)
        
        print(f"new connection: {warehouse} {database} {schema}")

        cur = conn.cursor()
        cur.execute("SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE from COLUMNS")
        
        num_batches = 0
        total_rows = 0
        while True:
            result_rows = cur.fetchmany(batch_size)
            num_result_rows = len(result_rows)
            if num_result_rows == 0:
                break
            for result_row in result_rows:
                
                table_name = result_row[0]
                table_column_count[table_name] = table_column_count.get(table_name, 0) + 1
                column_name = result_row[1]
                column_name_counts[column_name] = column_name_counts.get(column_name, 0) + 1
                
                if column_name in all_key_columns_set and not matches_any(table_name, table_name_filters):
                    key_columns_set = table_key_columns_sets.get(table_name, set())
                    key_columns_set.add(column_name)
                    table_key_columns_sets[table_name] = key_columns_set
                        
                print(result_row)
            total_rows += num_result_rows
            num_batches += 1
        
        print(f"\nfetched {total_rows} total_rows in {num_batches} batches")
        
        print(f"\nfound {total_rows} columns")
        print(f"\nfound {len(table_column_count)} tables")
        
        print("\ntop 10 table column counts")
        sorted_table_column_counts = sorted(table_column_count.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_table_column_counts[i])

        print("\ntop 10 column name counts")
        sorted_column_name_counts = sorted(column_name_counts.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_column_name_counts[i])
            
        print("\nkey column counts:")
        for key in all_key_columns_set:
            print(f"{key}: {column_name_counts.get(key)}")
        
        # set of all filtered table names that contain all key_columns
        tables_with_all_key_columns = set()

        print(f"\n{len(table_key_columns_sets.keys())} tables with any key_columns:", all_key_columns_set)
        for table_name, table_key_columns_set in table_key_columns_sets.items():
            if table_key_columns_set == all_key_columns_set:
                assert table_name not in tables_with_all_key_columns, f"ERROR: {table_name} already saved"
                tables_with_all_key_columns.add(table_name)
                print(table_name)
        
        return tables_with_all_key_columns
        
    except Exception as err:
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

# Return list of utc timestamped key_column_counts dicts for each table_entry. for example:
#
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
def get_table_entries_key_column_counts(tables_with_all_key_columns: Set[str]) -> List[Dict]:
    table_entries_key_column_counts = []
    
    job_datetime_str = datetime.datetime.utcnow().isoformat();
    sorted_table_entries = sorted(list(tables_with_all_key_columns))
    uncounted_table_entries = set(sorted_table_entries)
    counted_table_entries = set()
    
    parts = []
    for key in list(all_key_columns_set):
        parts.append(f"COUNT(DISTINCT({key}))")
    key_columns_select_clause = ", ".join(parts)
    
    parts = []
    for key in list(all_key_columns_set):
        parts.append( f"{key} is not NULL" )
    key_columns_where_clause = " and ".join(parts)

    warehouse = WAREHOUSE
    prev_database = None
    prev_schema = None
    conn = None
    cur = None
    
    batch_size = 10

    for table_entry in sorted_table_entries:
        
        try:
            parts = table_entry.split("__")
            if len(parts) != 3:
                print(f"skipping table_entry: {table_entry}")
                continue
            database, schema, table = parts
            
            if database != prev_database or schema != prev_schema:                    
                if conn is not None:
                    conn.close()
                    conn = None

            if conn is None:
                if cur is not None:
                    cur.close()
                    cur = None

                conn = connector.connect(
                    user=USER_NAME,
                    password=USER_PSWD,
                    account=ACCOUNT,
                    warehouse=WAREHOUSE,
                    database=database,
                    schema=schema,
                    protocol='https',
                    port=PORT)
                
                print(f"new connection: {warehouse} {database} {schema}")
                
                prev_database = database
                prev_schema = schema
        
            if cur is None:
                cur = conn.cursor()

            query = f"SELECT {key_columns_select_clause} FROM {table} WHERE {key_columns_where_clause}"
            cur.execute(query)

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
                        "datetime": job_datetime_str,
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
            
        except Exception as err:
            print(f"Error: {type(err)} {str(err)}")
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    print("\nskipped", len(uncounted_table_entries), "tables out of", len(sorted_table_entries) )
    for uncounted_table_entry in sorted(uncounted_table_entries):
        print("skipped", uncounted_table_entry)

    print("\npassed", len(counted_table_entries), "tables out of", len(sorted_table_entries) )
    for counted_table_entry in sorted(counted_table_entries):
        print("passed", counted_table_entry)

    assert len(table_entries_key_column_counts) == len(counted_table_entries), "ERROR: count failure"
    return table_entries_key_column_counts


def main():
    tables_with_all_key_columns = find_tables_with_key_columns()
    print("\nnum tables_with_all_key_columns:", len(tables_with_all_key_columns))
    
    table_entries_key_column_counts = get_table_entries_key_column_counts(tables_with_all_key_columns)
    print("\nnum table_entries_key_column_counts:", len(table_entries_key_column_counts))

if __name__ == "__main__":
    
    main()
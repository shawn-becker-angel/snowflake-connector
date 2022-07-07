import sys
# import pandas as pd
import snowflake.connector as connector
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import List, AnyStr

# returns True if name contains any filter in filters
def matches_any(name, filters) -> bool:
    for filter in filters:
        if filter in name:
            return True
    return False

# Returns a list of all filtered table_names that contain all key_columns
@timefunc
def find_tables_with_key_columns() -> List[AnyStr]:
    
    print("snowflake.connector: ", connector.__version__ )
    # print("pandas: ", pd.__version__ )
    print("python: ", sys.version)
    
    conn = connector.connect(
        user=USER_NAME,
        password=USER_PSWD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema='INFORMATION_SCHEMA',
        protocol='https',
        port=PORT
        )
    cur = conn.cursor()
    table_names = {}
    column_names = {}
    all_key_columns_set = set(["ANONYMOUS_ID", "USER_ID", "EMAIL"])
    table_key_columns_sets = {}  # { table_name:'table_name', key_columns_set: [key,key,key]}
    table_name_filters = set(["DEV","STAGING"])
    
    
    try:
        cur.execute("SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE from COLUMNS")
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status(query_id)):
            print("waiting")
            sleep(1)

        start = perf_counter()
        batch_size = 10
        batch_no = 0
        rows = 0
        while True:
            ret = cur.fetchmany(batch_size)
            ret_len = len(ret)
            if ret_len == 0:
                break
            for row in ret:
                
                table_name = row[0]
                table_names[table_name] = table_names.get(table_name, 0) + 1
                column_name = row[1]
                column_names[column_name] = column_names.get(column_name, 0) + 1
                
                if column_name in all_key_columns_set and not matches_any(table_name, table_name_filters):
                    key_columns_set = table_key_columns_sets.get(table_name, set())
                    key_columns_set.add(column_name)
                    table_key_columns_sets[table_name] = key_columns_set
                        
                print(row)
            rows += ret_len
            batch_no += 1
        
        elapsed = perf_counter() - start
        print(f"\nfetched {rows} rows in {batch_no} batches in {elapsed:6.3f} seconds")
        
        print(f"\nfound {rows} columns")
        print(f"\nfound {len(table_names)} tables")
        
        print("\ntop 10 table counts")
        sorted_tables = sorted(table_names.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_tables[i])

        print("\ntop 10 column counts")
        sorted_columns = sorted(column_names.items(), key=lambda e: e[1], reverse=True)
        for i in range(10):
            print(sorted_columns[i])
            
        print("\nkey column counts:")
        for key in all_key_columns_set:
            print(f"{key}: {column_names.get(key)}")
        
        # list of all filtered table_names that contain all key_columns
        tables_with_key_columns = []

        print(f"\n{len(table_key_columns_sets.keys())} tables with all key_columns:", all_key_columns_set, "that don't match any:", table_name_filters)
        for table_name, table_key_columns_set in table_key_columns_sets.items():
            if table_key_columns_set == all_key_columns_set:
                tables_with_key_columns.append(table_name)
                print(table_name)
                
        return tables_with_key_columns
        

    except Exception as err:
        print(f"Error: {type(err)} {str(err)}")

    finally:
        cur.close()
        conn.close()

def main():
    tables_with_key_columns = find_tables_with_key_columns()
    print("\nnum tables_with_key_columns:", len(tables_with_key_columns))

if __name__ == "__main__":
    
    main()
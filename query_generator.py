import sys
import pandas as pd
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from constants import *   
from timefunc import timefunc
from typing import List

def create_connector(verbose: bool=True):
    conn = connector.connect(
        user=USER_NAME,
        password=USER_PSWD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        protocol='https',
        port=PORT)

    if verbose:
        print(f"new connector: {WAREHOUSE} {DATABASE} {SCHEMA}")

    return conn


# Returns a generator function that 
# 1. executes a query
# 2. fetches a max of batch_size rows from the the total query result at a time
# 3. invokes 'yield' with that batch of rows
# 4. continues fetching batches until all rows are processed
# throws StopIteration when all rows have been fetched without error
# prints timeout error when query execution exceeds timeout_seconds
#
# Usage: see test_list_columns() function below
#  
def query_batch_generator(
    query: str, 
    conn: connector=None, 
    batch_size: int=DEFAULT_BATCH_SIZE, 
    timeout_seconds: int=DEFAULT_TIMEOUT_SECONDS, 
    verbose: bool=False):
    try:
        close_conn = False
        if conn is None:
            conn = create_connector()
            close_conn = True
        cur = conn.cursor()
        
        assert query is not None, "ERROR: undefined query"
        
        cur.execute(query, timeout=timeout_seconds)
        
        num_batches = 0
        total_rows = 0
        while True:
            batch_rows = cur.fetchmany(batch_size)
            num_batch_rows = len(batch_rows)
            if num_batch_rows == 0:
                break
            
            yield batch_rows
            
            total_rows += num_batch_rows
            num_batches += 1
        
        if verbose:
            print(f"yielded {total_rows} total_rows in {num_batches} batches")

    except ProgrammingError as err:
        if err.errno == 604:
            print(timeout_seconds, "second timeout for query:\n", query)
        else:
            print(f"Error: {type(err)} {str(err)}")
    finally:
        cur.close()
        if close_conn:
            conn.close()

def execute_count_query(
    count_query: str, 
    conn: connector=None, 
    timeout_seconds: int=DEFAULT_TIMEOUT_SECONDS, 
    verbose: bool=False) -> int:
    
    query_batch_iterator = query_batch_generator(count_query, conn=conn, timeout_seconds=timeout_seconds, batch_size=1, verbose=verbose)
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
                count = result_row[0]
                return count
        except StopIteration:
            break
    return 0

def clean_query(query: str) -> str:
    # strip external white-spaces
    query = query.strip()
    # replace multiple internal white-spaces with single white-space
    return " ".join(query.split())

def execute_batched_select_query(
    select_query: str, 
    select_columns: List[str], 
    conn: connector=None, 
    timeout_seconds: int=DEFAULT_TIMEOUT_SECONDS, 
    batch_size: int=DEFAULT_BATCH_SIZE, 
    batch_dot_frequency: int=100,
    batch_dot: str='.',
    verbose: bool=False) -> pd.DataFrame:
    
    if verbose:
        print(f"execute_batched_select_query.select_query:\n{clean_query(select_query)};")
        print(f"execute_batched_select_query.select_columns:\n{select_columns}")
    
    union_df = pd.DataFrame(columns=select_columns)
    query_batch_iterator = query_batch_generator(select_query, conn=conn, timeout_seconds=timeout_seconds, batch_size=batch_size, verbose=verbose)
    num_batches = 0
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            batch_df = pd.DataFrame(columns=select_columns, data=batch_rows)
            batch_df = batch_df.drop_duplicates(keep="first")
            union_df = pd.concat([union_df, batch_df], axis=0)
            num_batches += 1
            if batch_dot_frequency is not None and batch_dot_frequency>0 and batch_dot is not None and num_batches % batch_dot_frequency == 0:
                sys.stdout.write(batch_dot)
                sys.stdout.flush
                
        except StopIteration:
            break
    union_df = union_df.drop_duplicates(keep="first")
    return union_df

################################################
# Tests
################################################

@timefunc
def test_list_columns_1():
    conn = create_connector()
    query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS LIMIT 10"
    query_batch_iterator = query_batch_generator(query, conn=conn, verbose=False)
    while True:
        try:
            batch_rows = next(query_batch_iterator)
            for result_row in batch_rows:
                print(result_row)
        except StopIteration:
            break

@timefunc
def test_list_columns_2():
    conn = create_connector()
    select_columns = ['TABLE_NAME', 'COLUMN_NAME']
    select_query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS LIMIT 10"
    df = execute_batched_select_query(select_query, select_columns, conn=conn)
    for row in list(df.values):
        print(row)

@timefunc
def tests():
    
    test_list_columns_2()
    
    test_list_columns_1()

    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()

if __name__ == "__main__":
    main()
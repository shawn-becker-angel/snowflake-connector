import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from constants import *   
from timefunc import timefunc


def create_connector():
    conn = connector.connect(
        user=USER_NAME,
        password=USER_PSWD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        protocol='https',
        port=PORT)
    return conn

# a generator function that executes a query, loads a batch, 
# and calls yield with result_row for each result row
# Usage: 
def query_row_processor(query: str, conn: connector=None, batch_size: int=10, timeout_seconds: int=10, verbose: bool=False):
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
            result_rows = cur.fetchmany(batch_size)
            num_result_rows = len(result_rows)
            if num_result_rows == 0:
                break
            
            for result_row in result_rows:
                yield result_row

            total_rows += num_result_rows
            num_batches += 1
        
        if verbose:
            print(f"\nprocessed {total_rows} total_rows in {num_batches} batches")

    except ProgrammingError as err:
        if err.errno == 604:
            print(timeout_seconds, "second timeout for query:\n", query)
        else:
            print(f"Error: {type(err)} {str(err)}")
    finally:
        cur.close()
        if close_conn:
            conn.close()

def test_list_columns():
    conn = create_connector()
    query = "SELECT TABLE_NAME, COLUMN_NAME FROM LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS"
    it = query_row_processor(query, conn=conn, verbose=True)
    while True:
        try:
            result_row = next(it)
            print(result_row)
        except StopIteration:
            break
        
@timefunc
def main():
    test_list_columns()

if __name__ == "__main__":
    main()
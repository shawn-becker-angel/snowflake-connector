import sys
# import pandas as pd
import snowflake.connector as connector
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter

@timefunc
def main():
    
    print("snowflake.connector: ", connector.__version__ )
    # print("pandas: ", pd.__version__ )
    print("python: ", sys.version)
    
    conn = connector.connect(
        user=USER_NAME,
        password=USER_PSWD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        protocol='https',
        port=PORT
        )
    cur = conn.cursor()
    try:
        cur.execute("SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE from LOOKER_SOURCE.INFORMATION_SCHEMA.COLUMNS")
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status(query_id)):
            print("waiting")
            sleep(1)

        start = perf_counter()
        batch_size = 100
        batch_number = 0
        total_row_columns = 0
        while True:
            rows = cur.fetchmany(batch_size)
            num_rows = len(rows)
            if num_rows == 0:
                break
            for row in rows:
                print(row)
            total_row_columns += num_rows
            batch_number += 1
        
        elapsed = perf_counter() - start
        print(f"fetched {total_row_columns} columns in {batch_number} batches in {elapsed:6.3f} seconds")

    except Exception as err:
        print(f"Error: {type(err)} {str(err)}")

    finally:
        cur.close()
        print("cursor closed")
        conn.close()
        print("connection closed")


if __name__ == "__main__":
    main()
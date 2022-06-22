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
        schema='INFORMATION_SCHEMA',
        protocol='https',
        port=PORT
        )
    cur = conn.cursor()
    try:
        cur.execute("SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE from COLUMNS")
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status(query_id)):
            print("waiting")
            time.sleep(1)

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
                print(row)
            rows += ret_len
            batch_no += 1
        
        elapsed = perf_counter() - start
        print(f"fetched rows:{rows} in batches:{batch_no} in {elapsed:10.3f} seconds")

    except ProgrammingError as err:
        print(f"Error: {type(err)} {str(err)}")

    finally:
        cur.close()
        print("cursor closed")
        conn.close()
        print("connection closed")


if __name__ == "__main__":
    main()
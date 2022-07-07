# python snowflake connector - with pandas

## Install pandas packages

install the pandas package and extension:  (not yet working for Apple M1 pro chip)  
`pip install pandas`  
`pip install "snowflake-connector-python[pandas]"`  
**see open Apple M1 pro chip issue at: [https://github.com/snowflakedb/snowflake-connector-python/issues/986]**

## Run the validator to check the connector version from snowflake  

`> python validate.py`  
`snowflake.connector version: 6.19.0`  

## Run the example app

`> python example.py`  
`...`  
`fetched 42548 rows in 4255 batches in  1.769 seconds`  

## Coding example (with built-in pandas if working)  

Use `fetch_pandas_batches()` to fetch snowflake data into batched dataframes

```python
    import pandas as pd
    def fetch_pandas(cur, sql):
        batch_size = 1_000

        cur.execute(sql)

        start = perf_counter()
        batch_no = 0
        rows = 0
        while True:
            dat = cur.fetchmany(batch_size)
            if not dat:
                break
            df = pd.DataFrame(dat, columns=cur.description)
            rows += df.shape[0]
            batch_no += 1

        elapsed = perf_counter() - start
        print(f"fetched rows:{rows} in batches:{batch_no} in {elapsed:10.3f} secs")
```

## Coding example (without built-in pandas)  

```python
    import snowflake.connector as connector
    from time import sleep, perf_counter

    # uses dotenv.load_env() and os.getenv(key) to get secret values 
    # from local `.env` file, which should never be saved to github
    from constants import * 

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
        # execute the query
        cur.execute("SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE from COLUMNS")

        # wait for the query to finish
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status(query_id)):
            print("waiting")
            time.sleep(1)

        # read and process the results in batches
        start = perf_counter()
        batch_size = 1_000
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
        print(f"fetched rows:{rows} in batches:{batch_no} in {elapsed:10.3f} secs")

    except ProgrammingError as err:
        print(f"Error: {type(err)} {str(err)}")

    finally:
        cur.close()
        print("cursor closed")
        conn.close()
        print("connection closed")

```

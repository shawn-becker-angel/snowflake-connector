# python snowflake connector

## Download and install miniconda

as instructed here:
`https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html`

create a virtual conda environment:  
`conda create -n snowflake python=3.9`

activate your environment:  
`conda activate snowflake`

verify your python version:  
`python --version`

upgrade pip:  
`python -m pip install --upgrade pip`

verify that conda environment bin has been added to PATH:  
`export PATH=$CONDA_PYTHON_PREFIX/bin:$PATH`

add conda environment site-packages to PYTHONPATH:  
`export PYTHONPATH=$CONDA_PREFIX/lib/python3.9/site-packages:$PYTHONPATH`

## Install snowflake python connector

as instructed here:  
`https://docs.snowflake.com/en/user-guide/python-connector.html`

create a local requirements.txt file by choosing a tested requirements files for your python version from  
`https://github.com/snowflakedb/snowflake-connector-python/tree/main/tested_requirements`

use curl to download your selected requirements.reqs file to a local requirements.txt file:  
`curl -o requirements-3.9.txt https://github.com/snowflakedb/snowflake-connector-python/blob/main/tested_requirements/requirements_39.reqs`

use the requirements-3.9.txt file to install all tested dependencies:  
`pip install -r requirements-3.9.txt`

check the version of the python connector from the last line of the requirements file:  
`tail -n 1 requirements-3.9.txt`

install the pandas extension:  
`pip install "snowflake-connector-python[pandas]"`

## Install other packages

`pip install pandas`
`pip install python-dotenv`

## Coding with pandas (if pandas is working)

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

## Coding without pandas (if pandas is not available)

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

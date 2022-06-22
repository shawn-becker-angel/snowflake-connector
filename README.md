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

## Coding with pandas

Connector API uses Connection and Cursor objects  

Use `fetch_pandas_all()` and `fetch_pandas_batches()` to fetch snowflake data into pandas dataframes

```python
    import pandas as pd
    def fetch_pandas(cur, sql):
        cur.execute(sql)
        rows = 0
        while True:
            dat = cur.fetchmany(50000)
            if not dat:
                break
            df = pd.DataFrame(dat, columns=cur.description)
            rows += df.shape[0]
        print(rows)
```

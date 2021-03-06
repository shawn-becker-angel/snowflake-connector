# python snowflake connector - installation

## Download and install miniconda

Tested on environment:

```bash
macOS Monterey 12.4
Apple M1 pro chip
Python 3.9
Visual Studio Code 1.68.1
```

as instructed here:  
[https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html]

create a virtual conda environment:  
`conda create -n snowflake python=3.9`

activate your environment:  
`conda activate snowflake`

verify your python version:  
`python --version`

upgrade pip:  
`python -m pip install --upgrade pip`  
## Install snowflake python connector  

as instructed here:  
[https://docs.snowflake.com/en/user-guide/python-connector.html]

create a local requirements.txt file by choosing a tested requirements files for your python version from  
[https://github.com/snowflakedb/snowflake-connector-python/tree/main/tested_requirements]

use curl to download your selected requirements.reqs file to a local requirements.txt file:  
`curl -o requirements-3.9.txt https://github.com/snowflakedb/snowflake-connector-python/blob/main/tested_requirements/requirements_39.reqs`

use the requirements-3.9.txt file to install all tested dependencies:  
`pip install -r requirements-3.9.txt`

check the version of the python connector from the last line of the requirements file:  
`tail -n 1 requirements-3.9.txt`

install other packages:  
`pip install python-dotenv`

install the pandas package and extension:  (not yet working for Apple M1 pro chip)  
`pip install pandas`  
`pip install "snowflake-connector-python[pandas]"`  
**see open Apple M1 pro chip issue at: [https://github.com/snowflakedb/snowflake-connector-python/issues/986]**

## Run the validator to check the connector version from snowflake  

`> python validate.py`  
`snowflake.connector version: 6.19.0`  


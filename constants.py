import os
from dotenv import load_dotenv
from utils import str2bool

if not os.path.isfile(".env"):
    raise Exception("local .env file not found")

load_dotenv()

USER_NAME = os.getenv("USER_NAME")
USER_PSWD = os.getenv("USER_PSWD")
ACCOUNT = os.getenv("ACCOUNT")
ROLE = os.getenv("ROLE")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")

DEFAULT_TIMEOUT_SECONDS = int(os.getenv("DEFAULT_TIMEOUT_SECONDS"))
DEFAULT_BATCH_SIZE = int(os.getenv("DEFAULT_BATCH_SIZE"))

# segment_table and columns_set constants
ALL_KEY_COLUMNS = set(["ID","ANONYMOUS_ID", "USER_ID", "EMAIL"])
ALL_TIMESTAMP_COLUMNS = set(["RECEIVED_AT","SENT_AT","TIMESTAMP"])
ALL_SEARCH_COLUMNS = set([*ALL_KEY_COLUMNS, *ALL_TIMESTAMP_COLUMNS,"RID"])
ALL_KEY_COLUMNS_STR = "-".join(ALL_KEY_COLUMNS)
ALL_SEARCH_COLUMNS_STR = "-".join(ALL_SEARCH_COLUMNS)

SEARCH_SEGMENT_TABLES_SET = set(["IDENTIFIES"])
SEARCH_IGNORE_SEGMENT_TABLES_SET = set(["DEV","STAGING","IDENTIFIES_METADATA"])

SEGMENT_TABLE_DICTS_DF_COLUMNS = ["segment_table","metadata_table","columns"]
SEGMENT_TABLE_DICTS_DF_DEFAULT_BASE_NAME = "segments_table_dicts_df"

# used by batch_segment_table_metadata.py
SEGMENT_METADATA = "SEGMENT.IDENTIFIES_METADATA"
BATCH_SEGMENT_TABLE_METADATA = "batch_segment_table_metadata"

SEGMENT_UUIDS = ['USER_ID_UUID','USERNAME_UUID','PERSONA_UUID','RID_UUID']
SEGMENT_QUERY_NAMES = [x.replace("UUID","QUERY").lower() for x in SEGMENT_UUIDS]
SEGMENT_QUERY_BATCH_SIZE = 1000
SEGMENT_QUERY_TIMEOUT_SECONDS = 60
SEGMENT_QUERY_LIMIT_CLAUSE = ''

# used by data_frame_utils.py
CSV_FORMAT = "csv"
PARQUET_FORMAT = "parquet"

################################################
# Tests
################################################

def test_constants():    
    print("all tests passed in", os.path.basename(__file__))

def tests():
    test_constants()

def main():
    tests()

if __name__ == "__main__":
    main()
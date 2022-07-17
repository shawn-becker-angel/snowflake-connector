import os
from dotenv import load_dotenv
from utils import str2bool

if not os.path.isfile(".env"):
    raise Exception("local .env file not found")

load_dotenv()

USER_NAME = os.getenv('USER_NAME')
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
ALL_SEARCH_COLUMNS = set([*ALL_KEY_COLUMNS, "RID"])
ALL_KEY_COLUMNS_STR = '-'.join(ALL_KEY_COLUMNS)
ALL_SEARCH_COLUMNS_STR = '-'.join(ALL_SEARCH_COLUMNS)

SEARCH_SEGMENT_TABLES_SET = set(['IDENTIFIES'])
SEARCH_IGNORE_SEGMENT_TABLES_SET = set(["DEV","STAGING"])

KEY_COLUMN_COUNTS_MIN_TIMESTAMP = "'2022-01-01'"
KEY_COLUMN_COUNTS_MAX_TIMESTAMP = "'2022-07-08'"
SEGMENT_TABLES_THAT_TIMEOUT = [
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF',
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_GNRL',
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_MINUTES',
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_TOTAL_MINUTES']

SEGMENT_TABLES_DF_COLUMNS = ['segment_table','columns']
SEGMENT_TABLES_DF_DEFAULT_BASE_NAME = 'segments_table_df'

AUGMENTED_SEGMENT_USERS_DF_DEFAULT_BASE_NAME = 'augmented_segment_users_df'

ELLIS_ISLAND_USERS_DF_COLUMNS = ["USER_UUID","USER_USERNAME", "USER_EMAIL"]
ELLIS_ISLAND_USERS_DF_DEFAULT_BASE_NAME = 'ellis_island_users_df'

USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE = str2bool(os.getenv("USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE"))

################################################
# Tests
################################################

def test_constants():
    assert isinstance(USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE, bool), f"ERROR type failure {type(USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE)}"
    
    print("all tests passed in", os.path.basename(__file__))

def tests():
    test_constants()

def main():
    tests()

if __name__ == "__main__":
    main()
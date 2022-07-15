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

USE_LATEST_SEGMENT_TABLES_CSV_FILE = str2bool(os.getenv("USE_LATEST_SEGMENT_TABLES_CSV_FILE"))
USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE = str2bool(os.getenv("USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE"))
USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE = str2bool(os.getenv("USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE"))

# more constants
ALL_KEY_COLUMNS_SET = set(["ANONYMOUS_ID", "USER_ID", "EMAIL"])
ALL_KEY_COLUMNS_LIST = sorted(list(ALL_KEY_COLUMNS_SET))
KEY_COLUMN_COUNTS_MIN_TIMESTAMP = "'2022-07-01'"
KEY_COLUMN_COUNTS_MAX_TIMESTAMP = "'2022-07-08'"
SEGMENT_TABLES_THAT_TIMEOUT = [
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF',
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_GNRL',
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_MINUTES',
    'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF_TOTAL_MINUTES']


################################################
# Tests
################################################

def test_constants():
    assert isinstance(USE_LATEST_SEGMENT_TABLES_CSV_FILE, bool), f"ERROR type failure {type(USE_LATEST_SEGMENT_TABLES_CSV_FILE)}"
    assert isinstance(USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE, bool), f"ERROR type failure {type(USE_LATEST_ELLIS_ISLAND_USERS_CSV_FILE)}"
    assert isinstance(USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE, bool), f"ERROR type failure {type(USE_LATEST_KEY_COLUMN_INFOS_CSV_FILE)}"
    
    print("all tests passed")

def tests():
    test_constants()

def main():
    tests()

if __name__ == "__main__":
    main()
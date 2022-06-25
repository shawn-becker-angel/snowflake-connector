import os
from dotenv import load_dotenv

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

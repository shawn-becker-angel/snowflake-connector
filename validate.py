
import snowflake.connector

from constants import *

# Gets the version
conn = snowflake.connector.connect(
    user=USER_NAME,
    password=USER_PSWD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
curs = conn.cursor()
try:
    curs.execute("SELECT current_version()")
    one_row = curs.fetchone()
    print("snowflake.connector version:", one_row[0])
    print("CONNECTION VALIDATED")
finally:
    curs.close()
conn.close()

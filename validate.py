
import snowflake.connector

from constants import *

# Gets the version
ctx = snowflake.connector.connect(
    user=USER_NAME,
    password=USER_PSWD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

import sys
from numpy import diff
import pandas as pd
import snowflake.connector as connector
from constants import *   
import math
import pprint

# adapted from postgresql code at 
# https://github.com/Angel-Studios/chosen-hydra/blob/master/lib/chosen_hydra/jobs/global_season_funding.ex#L109-L156

# NOTE
# NOTE THAT THIS snowflake-python version DOES NOT YET IMPLEMENT THE FIRST 89 LINES of global_season_funding.ex
# NOTE

# from STITCH__HYDRABIM__MARKET_PURCHASEDOFFER as p
# join STITCH__HYDRABIM__MARKET_ORDER

set_session_parameters_queries = """
ALTER SESSION SET TIMEZONE = 'Etc/UTC';
show parameters in session;
"""

def set_session_parameters(conn):
    with conn.cursor() as cur:
        try:
            sql_lines = set_session_parameters_queries.split("\n")
            for sql_line in sql_lines:
                if len(sql_line) > 0:
                    cur.execute(sql_line)
                    ret = cur.fetchone()
                    print(ret, sql_line)
        except Exception as err:
            print(f"Error: {type(err)} {str(err)}")


query = """
with orders as (
    
     select s.a as episode, 
        (s.a - 1) * $episode_cost_in_pennies as ep_start,  
        s.a * $episode_cost_in_pennies as ep_end 
        from (select row_number() over (order by seq4()) from table(generator(rowcount => $number_of_episodes))) as s(a)
    )
    select p.created, (offer:price)::int as price_paid, o.user_id
    from STITCH__HYDRABIM__MARKET_PURCHASEDOFFER as p
    join STITCH__HYDRABIM__MARKET_ORDER as o on o.id = p.order_id
    where p.created > '2001-01-01'::date
    AND refunded = FALSE
    AND (project_id = 'the-chosen' or project_id is null)
    )
    select
        created, price_paid, user_id from orders
        limit 1
"""


def calc_df(conn):
    with conn.cursor() as cur:
        try:
            cur.execute(query)
            batch_size = 1_000
            rows = 0
            df_total = None
            while True:
                dat = cur.fetchmany(batch_size)
                if len(dat) < 1:
                    break
                cols = [cur.description[i][0] for i in range(len(cur.description))]                    
                df = pd.DataFrame(dat, columns=cols)
                if df_total is None:
                    df_total = df
                else:
                    df_total = pd.concat([df_total, df], axis=0)
                rows += df.shape[0]
            return df_total
  
        except Exception as err:
            print(f"Error: {type(err)} {str(err)}")

def main():
    
    print("snowflake.connector: ", connector.__version__ )
    print("pandas: ", pd.__version__ )
    print("python: ", sys.version)
    
    try:
        with connector.connect(
            user=USER_NAME,
            password=USER_PSWD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema='PUBLIC',
            protocol='https',
            port=PORT
        ) as conn:
        
            set_session_parameters(conn)
            df = calc_df(conn)
            pprint.pprint(df)

            
    except Exception as err:
        print(f"Error: {type(err)} {str(err)}")


if __name__ == "__main__":
    main()
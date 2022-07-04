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


set_variables_queries = """
set season_three_completed_on = '2021-12-22';
set start_funding_slowdown_date = '2022-06-17';
set season_required = 24000000;
set number_of_episodes = 8;
set episode_cost = $season_required / $number_of_episodes;
set episode_cost_in_pennies = $episode_cost * 100;
set season_currently_funding = 4;
set episode_time_unit = 'Day';
set currency = 'USD';
show variables;
"""
  
def set_variables(conn):
    with conn.cursor() as cur:
        try:
            sql_lines = set_variables_queries.split("\n")
            for sql_line in sql_lines:
                if len(sql_line) > 0:
                    cur.execute(sql_line)
                    ret = cur.fetchone()
                    print(ret, sql_line)
        except Exception as err:
            print(f"Error: {type(err)} {str(err)}")

query = """
with orders as (
    select p.created, (offer:price)::int as price_paid, o.user_id
    from STITCH__HYDRABIM__MARKET_PURCHASEDOFFER as p
    join STITCH__HYDRABIM__MARKET_ORDER as o on o.id = p.order_id
    where p.created > $season_three_completed_on::date
    AND refunded = FALSE
    AND (project_id = 'the-chosen' or project_id is null)
    ), running_totals as (
    select created,
        sum(price_paid * case
            when created < $start_funding_slowdown_date::date then 0.3
            else 0.2
            end) over (order by created asc rows between unbounded preceding and current row)
        as running_total,
        user_id
    from orders
    ), episodes as (
    select s.a as episode, 
        (s.a - 1) * $episode_cost_in_pennies as ep_start,  
        s.a * $episode_cost_in_pennies as ep_end 
        from (select row_number() over (order by seq4()) from table(generator(rowcount => $number_of_episodes))) as s(a)
    ), funding_episodes as (
    select episode,
        episodes.ep_end,
        episodes.ep_start,
        count(distinct(user_id)) as num_of_users,
        max(running_total) as total_raised,
        max(running_total) - episodes.ep_start as raised,
        (max(running_total) - episodes.ep_start) / $episode_cost_in_pennies as percent_complete
        from episodes left join lateral (
        select user_id, running_total from running_totals
        where running_totals.running_total between episodes.ep_start and episodes.ep_end
        ) -- ep_counts on TRUE
        group by episode, episodes.ep_end, episodes.ep_start
        order by episode, episodes.ep_end, episodes.ep_start
    )
    select
        -- input vars
        $season_required as season_required, 
        $number_of_episodes as number_of_episodes,
        $episode_cost as episode_cost,
        $episode_cost_in_pennies as episode_cost_in_pennies,
        $season_currently_funding as season_currently_funding,
        $episode_time_unit as episode_time_unit,
        $currency as currency,
        -- originals
        episode,
        num_of_users,
        (raised / 100)::int as raised,
        (percent_complete * 100.0)::int,
        (total_raised / 100)::int as total_raised,
        (select 
            (sum (IFF(created > DATEADD($episode_time_unit,-7,CURRENT_DATE()), price_paid, 0 )) / 100 * .3)::int 
            from orders) as past_seven_days,
        (select count(distinct(user_id)) from orders) as total_users
        from funding_episodes
        where raised is not null
        and percent_complete <= 0.99
        limit 1
"""

def calc_funding_result(df):
    season_required, number_of_episodes, episode_cost, episode_cost_in_pennies, season_currently_funding, episode_time_unit, currency, \
    episode, episode_backers, episode_raised, episode_percent_complete, total_raised, past_seven_days, total_backers = df.iloc[0,:]

    past_seven_days = 0 if past_seven_days is None else past_seven_days
    
    total_amount_raised = total_raised
    episodes_funded = episode
    episode_currently_funded = episode_raised
    episode_currently_funding = episodes_funded

    episode_hours_remaining = \
        (episode_cost - episode_currently_funded) / max(past_seven_days / (24 * 7), 1)

    (episode_time_left, episode_time_unit) = \
        (math.ceil(episode_hours_remaining), 'Hours') \
        if episode_hours_remaining < 24 \
        else (math.floor(episode_hours_remaining / 24), 'Days')
        
    remaining = season_required - total_amount_raised
    progress = min(math.floor(total_amount_raised / season_required * 100), 100)
    hours_remaining = remaining / max(past_seven_days / (24 * 7), 1)

    (time_left, time_unit) = \
        (math.ceil(hours_remaining), 'Hours') \
        if hours_remaining < 24 \
        else (math.floor(hours_remaining / 24), 'Days')

    result = {
        "backers": total_backers,
        "progress": progress,
        "time_left": time_left,
        "time_unit": time_unit,
        "days": math.floor(hours_remaining / 24),
        "total_amount_raised": total_amount_raised,
        "episode_currently_funding": episode_currently_funding,
        "episode_raised": episode_currently_funded,
        "episode_funding_in": episode_time_left,
        "episode_funding_time_unit": episode_time_unit,
        "episode_percent_complete": episode_percent_complete,
        "episode_cost": episode_cost,
        "episode_backers": episode_backers,
        "currency": "USD",
        "season_currently_funding": season_currently_funding
    }
    return result

def calc_funding_df(conn):
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
            
# def rollback_episode(result):
#     episode_percent_completed = result['episode_percent_complete']
#     number_of_episodes = result['number_of_episodes']

#     previous_results = ChosenHydra.Jobs.GlobalSeasonFunding.funding()

#     if result['episode_currently_funding'] == 1:
#         updated_results = {
#             "episode_percent_complete" : 100,
#             "episode_currently_funding" : number_of_episodes,
#             "season_currently_funding" : result["season_currently_funding"] - 1,
#             "episode_raised" : math.trunc(result["episode_cost"]),
#             "progress" : 100,
#             "time_left" : 0,
#             "days" : 0,
#             "total_amount_raised" : math.trunc(number_of_episodes * result["episode_cost"]),
#             "episode_funding_in" : 0,
#             "episode_backers" : previous_results["episode_backers"]
#         }

#         current_episode = {
#             "episode_percent_complete" : 100,
#             "episode_currently_funding" : current_episode - 1,
#             "episode_raised" : math.trunc(result["episode_cost"]),
#             "time_left" : 0,
#             "days" : 0,
#             "total_amount_raised" : math.trunc((current_episode - 1) * result["episode_cost"]),
#             "episode_funding_in" : 0,
#             "episode_backers" : previous_results["episode_backers"]
#         }
#     map.merge(result, updated_results) # ??

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
            set_variables(conn)
            df = calc_funding_df(conn)
            result = calc_funding_result(df)
            pprint.pprint(result)

            # # if episode is <1% funded, then instead show previous episode as fully funded
            # result = rollback_episode(result)
            # pprint.pprint(result)

            # ChosenHydra.Jobs.GlobalSeasonFunding.refresh(result)
            
    except Exception as err:
        print(f"Error: {type(err)} {str(err)}")


if __name__ == "__main__":
    main()
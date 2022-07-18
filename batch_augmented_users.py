from typing import Dict
from constants import *
import snowflake.connector as connector
from query_generator import create_connector, execute_batched_select_query
import pandas as pd
from segment_tables import get_first_timestamp_column_in_column_set, get_segment_tables_df, get_segment_table_dicts, remove_extra_timestamp_columns_in_column_set

def find_ellis_island_uuids_for_identifies_user_ids(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Dict[str, pd.DataFrame]:

    # return a dictionary of query_names -> DataFrame
    named_data_frames = {}
    
    segment_table = segment_table_dict['segment_table']
    segment_users_columns = set(segment_table_dict['columns'].split("-"))
    
    # keep only the first timestamp column in segment_users_columns set
    first_timestamp_column = get_first_timestamp_column_in_column_set(segment_users_columns)
    segment_users_columns = remove_extra_timestamp_columns_in_column_set(segment_users_columns)
        
    segment_users_columns = list( segment_users_columns)
    select_clause = ', '.join([f"id.{x}" for x in segment_users_columns]) + ', ei.uuid as valid_uuid'
    timestamp_clause = f" and {first_timestamp_column} >= '2021-04-01'" if first_timestamp_column is not None else ""
    segment_users_columns.append('valid_uuid')

    identifies_table = segment_table
    ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
    persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
    watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"

    # if identifies_table has RID column
    if "RID" in segment_users_columns:
        rid_query = f"\
         select distinct {select_clause}, id.rid\
            from {identifies_table} id\
            join {watchtime_table} wt\
                on id.rid = wt.rid\
            join {ellis_island_table} ei\
                on wt.user_id = ei.uuid\
            where ei.uuid is not NULL {timestamp_clause}"
            
        rid_df = execute_batched_select_query(rid_query, segment_users_columns, conn=conn)
        named_data_frames[f'{segment_table}_rid_df'] = rid_df
        
    user_id_query = f"\
        select distinct {select_clause}\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.user_id = ei.uuid\
        where ei.uuid is not NULL {timestamp_clause}"
    
    user_id_df = execute_batched_select_query(user_id_query, segment_users_columns, conn=conn, verbose=verbose)
    named_data_frames[f'{segment_table}_user_id_df'] = user_id_df
        
    email_username_query = f"\
         select distinct {select_clause}\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.email = ei.username\
        where ei.uuid is not NULL {timestamp_clause}"
    
    email_username_df = execute_batched_select_query(email_username_query, segment_users_columns, conn=conn)
    named_data_frames[f'{segment_table}_email_username_df'] = email_username_df

    user_id_persona_query = f"\
         select distinct {select_clause}\
        from {identifies_table} id\
        join {persona_users_table} pu\
            on id.user_id = pu.id  \
        join {ellis_island_table} ei\
            on pu.id = ei.uuid\
        where ei.uuid is not NULL {timestamp_clause}"
    
    user_id_persona_df = execute_batched_select_query(user_id_persona_query, segment_users_columns, conn=conn)
    named_data_frames[f'{segment_table}_user_id_persona_df'] = user_id_persona_df
        
    return named_data_frames

def compute_ellis_island_uuids_for_all_segment_tables(conn: connector=None, verbose: bool=True) -> None:
    segment_tables_df = get_segment_tables_df(load_latest=False, verbose=verbose)
    
    # converts 2-column dataframe to a list of 2-property dicts
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    if verbose:
        print("num segment_table_dicts:", len(segment_table_dicts))
    
    for segment_table_dict in segment_table_dicts:
        named_data_frames = find_ellis_island_uuids_for_identifies_user_ids(segment_table_dict, conn=conn, verbose=verbose)
        for name, df in named_data_frames.items():
            print(f"name:{name} df.shape:{df.shape} df.columns:{df.columns}")
            
    
    
################################################
# Tests
################################################

def test():
    conn = create_connector()
        
    print("compute_augmented_segment_users_df_for_all_segment_tables")
    compute_ellis_island_uuids_for_all_segment_tables(conn=conn, verbose=True)

def main():
    test()

if __name__ == "__main__":
    main()
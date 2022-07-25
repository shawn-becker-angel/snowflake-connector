from typing import Dict, List, Any
from constants import *
import snowflake.connector as connector
from query_generator import create_connector, execute_batched_select_query, execute_count_query, clean_query
import pandas as pd
from segment_tables import get_segment_tables_df, get_segment_table_dicts
from segment_utils import get_info_schema_table_name_from_segment_table
from data_frame_utils import save_data_frame
import pprint

# Given a segment_table_dict with the following structure:
# {
#     'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES'
#     'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
# }
#
# Returns a segment_table_metadata_dict with the following structure
# { 
#     "segment_table": <segment_table>,
#     "segment_queries": {
#           "user_id_query": {
#               "sql": <select_str>, "dataframe": <pd.DataFrame>
#           },
#           "username_query": {
#               "sql": <select_str>, "dataframe": <pd.DataFrame>
#           },
#           "persona_query": {
#               "sql": <select_str>, "dataframe": <pd.DataFrame>
#           },
#           "rid_query": {
#               "sql": <select_str>, "dataframe": <pd.DataFrame>
#           }
#     },
# },

def compute_segment_table_metadata_dict(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Dict[str,Any]:
    batch_size = 1000

    segment_table = segment_table_dict['segment_table']
    segment_table_metadata = {}
    segment_table_metadata["segment_table"] = segment_table
    segment_table_metadata["segment_queries"] = {}

    segment_table_columns = set(segment_table_dict['columns'].split("-"))
    segment_table_columns = list(segment_table_columns)
    select_clause = ', '.join([f"id.{x}" for x in segment_table_columns])
    not_null_clause = " and ".join([f"id.{x} is not NULL" for x in segment_table_columns])

    identifies_table = segment_table
    ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
    persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
    watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"

    #------------------------------------------------------------
    user_id_query_metadata = {}
    user_id_query = f"\
        select distinct {select_clause}, ei.uuid as user_id_uuid, current_timestamp() as user_id_ts\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.user_id = ei.uuid\
        where {not_null_clause}"

    user_id_query_metadata["sql"] = clean_query(user_id_query)
    user_id_columns = [*segment_table_columns, 'USER_ID_UUID', 'USER_ID_TS']
    user_id_df = execute_batched_select_query(user_id_query, user_id_columns, batch_size=batch_size, conn=conn, verbose=verbose)
    user_id_query_metadata["dataframe"] = user_id_df
    segment_table_metadata["segment_queries"]["user_id_query"] = user_id_query_metadata

    #------------------------------------------------------------
    username_query_metadata = {}
    username_query = f"\
        select distinct {select_clause}, ei.uuid as username_uuid, current_timestamp() as username_ts\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.email = ei.username\
        where {not_null_clause}"
        
    username_query_metadata["sql"] = clean_query(username_query)
    username_columns = [*segment_table_columns, 'USERNAME_UUID', 'USERNAME_TS']
    username_df = execute_batched_select_query(username_query, username_columns, batch_size=batch_size, conn=conn)
    username_query_metadata["dataframe"] = username_df
    segment_table_metadata["segment_queries"]["username_query"] = username_query_metadata

    #------------------------------------------------------------
    persona_query_metadata = {}
    persona_query = f"\
        select distinct {select_clause}, ei.uuid as persona_uuid, current_timestamp() as persona_ts\
        from {identifies_table} id\
        join {persona_users_table} pu\
            on id.user_id = pu.id  \
        join {ellis_island_table} ei\
            on pu.id = ei.uuid\
        where {not_null_clause}"
        
    persona_query_metadata["sql"] = clean_query(persona_query)
    persona_columns = [*segment_table_columns, 'PERSONA_UUID', 'PERSONA_TS']
    persona_df = execute_batched_select_query(persona_query, persona_columns, batch_size=batch_size, conn=conn)
    persona_query_metadata["dataframe"] = persona_df
    segment_table_metadata["segment_queries"]["persona_query"] = persona_query_metadata

    # if identifies_table has column 'RID'
    if "RID" in segment_table_columns:
        rid_query_metadata = {}
        rid_query = f"\
         select distinct {select_clause}, id.rid, ei.uuid as rid_uuid, current_timestamp() as rid_ts\
            from {identifies_table} id\
            join {watchtime_table} wt\
                on id.rid = wt.rid\
            join {ellis_island_table} ei\
                on wt.user_id = ei.uuid\
            where {not_null_clause} and id.rid is not NULL"
                
        rid_query_metadata["sql"] = clean_query(rid_query)
        rid_columns = [*segment_table_columns, 'RID_UUID', 'RID_TS']
        rid_df = execute_batched_select_query(rid_query, rid_columns, batch_size=batch_size, conn=conn)
        rid_query_metadata["dataframe"] = rid_df
        segment_table_metadata["segment_queries"]["rid_query"] = rid_query_metadata
    
    return segment_table_metadata


# Given a segment_table_metadata_dict, saves the dataframes from each segment query as a csv_file to disk
def save_segment_table_metadata_dict(segment_table_metadata_dict: Dict[str,Any], verbose: bool=True) -> None:
    segment_table = segment_table_metadata_dict['segment_table']
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    metadata_table = f"{SEGMENT_METADATA}.{info_schema_table_name}"
    csv_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_df"

    for query_name, query_values in segment_table_metadata_dict['segment_queries'].items():
        df = query_values["dataframe"]
        csv_file = save_data_frame(csv_base_name, df)
        view = {
            "metadata_table": metadata_table,
            "query_name": query_name,
            "shape":df.shape,
            "columns": df.columns,
            "csv_file": csv_file
        }
        if verbose:
            pprint.pprint(view)
        

# Loads the latest saved set of segment_tables, computes and saves dataframes of metadata for each segment_table to disk
def compute_and_save_metadata_for_all_segment_tables(conn: connector=None, verbose: bool=True) -> None:
    _, segment_tables_df = get_segment_tables_df(load_latest=True, verbose=verbose)
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    total_segment_tables =  len(segment_table_dicts)
    print("total_segment_tables:", total_segment_tables)
    
    cnt = 1
    for segment_table_dict in segment_table_dicts:
        print(f"computing segment_table_metadata ({cnt} out of {total_segment_tables})")
        segment_table_metadata_dict = compute_segment_table_metadata_dict(segment_table_dict, conn=conn, verbose=verbose)
        print(f"saving segment_table_metadat ({cnt} out of {total_segment_tables})")
        save_segment_table_metadata_dict(segment_table_metadata_dict, verbose=verbose)
        cnt += 1


################################################
# Tests
################################################

def test():
    conn = create_connector()
        
    print("compute_and_save_metadata_for_all_segment_tables")
    compute_and_save_metadata_for_all_segment_tables(conn=conn, verbose=True)

def main():
    test()

if __name__ == "__main__":
    main()
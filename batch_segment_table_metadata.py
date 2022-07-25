from typing import Dict, List, Any, Optional
from constants import *
import snowflake.connector as connector
from query_generator import create_connector, execute_batched_select_query, execute_count_query, clean_query
import pandas as pd
import pyarrow.feather as pf
from segment_tables import get_segment_tables_df, get_segment_table_dicts
from segment_utils import get_info_schema_table_name_from_segment_table
from data_frame_utils import save_data_frame, load_latest_data_frame, load_data_frame
import pprint
import json
import datetime
from utils import get_file_name_extension, find_latest_file


# Given a segment_table_dict with the following structure:
# {
#     'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES'
#     'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
# }
#
# Returns a segment_table_metadata_dict with the following structure:
# { 
#     "segment_table": <segment_table>,
#     "segment_queries": {
#           "user_id_query": <query_metadata>,
#           "username_query": <query_metadata>,
#           "persona_query": <query_metadata>,
#           "rid_query": <query_metadata>,
#     },
# },
#
# where <query_metadata> is:
# { "sql": <select_str>, "dataframe": <pd.DataFrame>, "data_file_name": <data_file_name> }
# NOTE that "dataframe" is empty when serialized to "data_file_name" and vice verse.

SEGMENT_QUERY_NAMES = ["user_id_query", "username_query", "persona_query", "rid_query"]
SEGMENT_QUERY_BATCH_SIZE = 100
SEGMENT_QUERY_TIMEOUT_SECONDS = 60

def compute_segment_table_metadata_dict(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Dict[str,Any]:

    segment_table = segment_table_dict['segment_table']
    segment_table_metadata = {}
    segment_table_metadata["segment_table"] = segment_table
    segment_table_metadata["segment_queries"] = {}

    segment_table_columns = set(segment_table_dict['columns'].split("-"))
    segment_table_columns = list(segment_table_columns)
    select_clause = ', '.join([f"id.{x}" for x in segment_table_columns])
    not_null_clause = " and ".join([f"id.{x} is not NULL" for x in segment_table_columns])
    limit_clause = "limit 100"

    identifies_table = segment_table
    ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
    persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
    watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"

    segment_table_metadata_dict = {}
    segment_table_metadata_dict['segment_table'] = segment_table

    #------------------------------------------------------------
    query_metadata = {}
    query_name = "user_id_query"
    query = f"\
        select distinct {select_clause}, ei.uuid as user_id_uuid, current_timestamp() as user_id_ts\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.user_id = ei.uuid\
        where {not_null_clause} {limit_clause}"

    query_metadata["sql"] = query = clean_query(query)
    columns = [*segment_table_columns, 'USER_ID_UUID', 'USER_ID_TS']
    df = execute_batched_select_query(
        query, columns, 
        batch_size=SEGMENT_QUERY_BATCH_SIZE, 
        timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
        conn=conn, verbose=verbose)
    query_metadata["dataframe"] = df
    segment_table_metadata["segment_queries"][query_name] = query_metadata

    #------------------------------------------------------------
    query_metadata = {}
    query_name = "username_query"
    query = f"\
        select distinct {select_clause}, ei.uuid as username_uuid, current_timestamp() as username_ts\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.email = ei.username\
        where {not_null_clause} {limit_clause}"
    
    query_metadata["sql"] = query = clean_query(query) 
    columns = [*segment_table_columns, 'USERNAME_UUID', 'USERNAME_TS']
    df = execute_batched_select_query(
        query, columns, 
        batch_size=SEGMENT_QUERY_BATCH_SIZE, 
        timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
        conn=conn, verbose=verbose)
    query_metadata["dataframe"] = df
    segment_table_metadata["segment_queries"][query_name] = query_metadata

    #------------------------------------------------------------
    query_metadata = {}
    query_name = "persona_query"
    query = f"\
        select distinct {select_clause}, ei.uuid as persona_uuid, current_timestamp() as persona_ts\
        from {identifies_table} id\
        join {persona_users_table} pu\
            on id.user_id = pu.id  \
        join {ellis_island_table} ei\
            on pu.id = ei.uuid\
        where {not_null_clause} {limit_clause}"
    
    query_metadata["sql"] = query = clean_query(query)
    columns = [*segment_table_columns, 'PERSONA_UUID', 'PERSONA_TS']
    df = execute_batched_select_query(
        query, columns, 
        batch_size=SEGMENT_QUERY_BATCH_SIZE, 
        timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
        conn=conn, verbose=verbose)
    query_metadata["dataframe"] = df
    segment_table_metadata["segment_queries"][query_name] = query_metadata

    # if identifies_table has column 'RID'
    if "RID" in segment_table_columns:
        #------------------------------------------------------------
        query_metadata = {}
        query_name = "rid_query"
        query = f"\
         select distinct {select_clause}, id.rid, ei.uuid as rid_uuid, current_timestamp() as rid_ts\
            from {identifies_table} id\
            join {watchtime_table} wt\
                on id.rid = wt.rid\
            join {ellis_island_table} ei\
                on wt.user_id = ei.uuid\
            where {not_null_clause} and id.rid is not NULL {limit_clause}"
              
        query_metadata["sql"] = query = clean_query(query)
        columns = [*segment_table_columns, 'RID_UUID', 'RID_TS']
        df = execute_batched_select_query(
            query, columns, 
            batch_size=SEGMENT_QUERY_BATCH_SIZE, 
            timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
            conn=conn, verbose=verbose)
        query_metadata["dataframe"] = df
        segment_table_metadata["segment_queries"][query_name] = query_metadata
    
    return segment_table_metadata


# Given a segment_table_metadata_dict, saves a timestamped json_metadata_file that includes 
# segment_name, segment_queries, as well as the timestamped data_file_name of the dataframe 
# that has been saved to local disk
def save_segment_table_metadata_dict(segment_table_metadata_dict: Dict[str,Any], verbose: bool=True) -> None:
    segment_table = segment_table_metadata_dict['segment_table']
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    metadata_table = f"{SEGMENT_METADATA}.{info_schema_table_name}"

    # saving one data_file and one json_metadata_file for each query_name
    
    for query_name in SEGMENT_QUERY_NAMES:
        query_metadata = segment_table_metadata_dict['segment_queries'][query_name]
        data_file_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_{query_name}_df"
        df = query_metadata["dataframe"]
        data_file_name = save_data_frame(data_file_base_name, df, format=FEATHER_FORMAT)
        query_metadata["data_file_name"] = data_file_name
        query_metadata["df"] = None
        if verbose:
            pprint.pprint(segment_table_metadata_dict)

    # dump the segment_table_metadata_dict to a local json file
    
    segment_table_metadata_dict_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_metadata_dict"
    segment_table_metadata_dict_json_file_name = f"{segment_table_metadata_dict_base_name}-{datetime.datetime.utcnow().isoformat}.json"
    with open(segment_table_metadata_dict_json_file_name, 'w') as f:
        json.dump(segment_table_metadata_dict, f)

# Given a segment_table_metadata_dict, loads the latest segment_table_metadata_dict json files for the given segment_table_dict.
# The segment_table_metadata_dict contains the data_file_name, from which each dataframe can be loaded 
def get_latest_segment_table_metadata_dict(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Optional[Dict[str,Any]]:

    segment_table_metadata_dict = None
    segment_table = segment_table_dict['segment_table']
    info_schema_table_name = get_info_schema_table_name_from_segment_table(segment_table)
    metadata_table = f"{SEGMENT_METADATA}.{info_schema_table_name}"
    
    # attempt to load the segment_table_metadata_dict from a local json file

    segment_table_metadata_dict_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_metadata_dict"
    segment_table_metadata_dict_json_file_name = find_latest_file(f"{segment_table_metadata_dict_base_name}-*.json")
    
    if segment_table_metadata_dict_json_file_name is not None:

        with open(segment_table_metadata_dict_json_file_name, 'r') as f:
            segment_table_metadata_dict = json.load(f)

        # deserialize the dataframe for each query_name
        for query_name in SEGMENT_QUERY_NAMES:
            query_metadata = segment_table_metadata_dict["segment_queries"][query_name]
            data_file_name = query_metadata["data_file_name"]
            df = query_metadata["data_frame"] = load_data_frame(data_file_name)
            assert df is not None
        
    return segment_table_metadata_dict

    
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

def get_latest_metadata_for_all_segment_tables(conn: connector=None, verbose: bool=True) -> None:
    _, segment_tables_df = get_segment_tables_df(load_latest=True, verbose=verbose)
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    total_segment_tables =  len(segment_table_dicts)
    print("total_segment_tables:", total_segment_tables)

    cnt = 1
    for segment_table_dict in segment_table_dicts:
        print(f"getting latest segment_table_metadata ({cnt} out of {total_segment_tables})")
        segment_table_metadata_dict = get_latest_segment_table_metadata_dict(segment_table_dict, conn=conn, verbose=verbose)
        cnt += 1


################################################
# Tests
################################################

def test():
    conn = create_connector()
        
    print("compute_and_save_metadata_for_all_segment_tables")
    compute_and_save_metadata_for_all_segment_tables(conn=conn, verbose=True)
    print("get_latest_metadata_for_all_segment_tables")
    get_latest_metadata_for_all_segment_tables(conn=conn, verbose=True)

def main():
    test()

if __name__ == "__main__":
    main()
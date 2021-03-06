from typing import Dict, Any, Optional
from constants import *
import snowflake.connector as connector
from query_generator import create_connector, execute_batched_select_query, clean_query
import pandas as pd
from segment_tables import get_segment_table_dicts_df, get_segment_table_dicts
from segment_utils import get_metadata_table_from_segment_table, get_uuid_column_from_segment_query_name
from data_frame_utils import save_data_frame, load_data_frame
import pprint
import json
import datetime
from utils import get_file_name_extension, find_latest_file


# Given a segment_table_dict with the following structure:
# {
#     'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES',
#     'metadata_table': 'SEGMENT__ANGEL_APP_IOS__IDENTIFIES',
#     'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
# }
#
# Returns a metadata_table_dict with the following structure:
# { 
#     "segment_table": <segment_table>,
#     "metadata_table": <metadata_table>,
#     "segment_queries": {
#           "user_id_query": <query_metadata>,
#           "username_query": <query_metadata>,
#           "persona_query": <query_metadata>,
#           "rid_query": <query_metadata>,
#     },
# },
#
# where <query_metadata> is:
# { "sql": <select_str>, "data_frame": <pd.DataFrame>, "data_file_name": <data_file_name> }
# NOTE that "data_frame" is empty when serialized to "data_file_name" and vice verse.

# Remove and replace the data_frame for each segment_query_name so the dict can be pprinted
def show_metadata_table_dict(metadata_table_dict: Dict[str,Any], caller: str=None) -> None:

    # temporarily save data_frame for each segment_query_name
    saved_data_frames = {}
    for segment_query_name in SEGMENT_QUERY_NAMES:
        if segment_query_name in metadata_table_dict['segment_queries'].keys():
            if "data_frame" in metadata_table_dict['segment_queries'][segment_query_name].keys():
                df = metadata_table_dict['segment_queries'][segment_query_name]["data_frame"]
                if df is not None:
                    uuid_name = get_uuid_column_from_segment_query_name(segment_query_name)
                    if uuid_name in df.columns:
                        # add extra temporary shape and UUID info for show only
                        uuid_nans = df[uuid_name].isna().sum()
                        metadata_table_dict['segment_queries'][segment_query_name]["shape"] = df.shape
                        metadata_table_dict['segment_queries'][segment_query_name]["uuid_name"] = uuid_name
                        metadata_table_dict['segment_queries'][segment_query_name]["uuid_nans"] = uuid_nans
                    saved_data_frames[segment_query_name] = df
                    metadata_table_dict['segment_queries'][segment_query_name]["data_frame"] = None
    
    if caller is not None:
        print("show_metadata_table_dict() called by", caller)
    pprint.pprint(metadata_table_dict)
    
    # restore data_frame for each segment_query_name
    for segment_query_name in SEGMENT_QUERY_NAMES:
        if segment_query_name in metadata_table_dict['segment_queries']:
            if "data_frame" in metadata_table_dict['segment_queries'][segment_query_name].keys():
                if segment_query_name in saved_data_frames.keys():
                    metadata_table_dict['segment_queries'][segment_query_name]["data_frame"] = saved_data_frames[segment_query_name]
                    # remove extra temporary shape and UUID info for show only
                    metadata_table_dict['segment_queries'][segment_query_name].pop("shape")
                    metadata_table_dict['segment_queries'][segment_query_name].pop("uuid_name")
                    metadata_table_dict['segment_queries'][segment_query_name].pop("uuid_nans")



# Executes the sql queries that compute up to 4 possible UUID values for the user_id in each row in 
# the given segment_table. The results of these queries are saved in memory as data_frames, which are 
# referenced in the returned metadata_table_dict.
# Related:
# see save_metadata_table_dict
# see load_latest_metadata_table_dict
def compute_metadata_table_dict(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Dict[str,Any]:
    segment_table = segment_table_dict['segment_table']
    metadata_table = segment_table_dict['metadata_table']
    metadata_table_dict = {}
    metadata_table_dict['segment_table'] = segment_table
    metadata_table_dict['metadata_table'] = metadata_table
    metadata_table_dict["segment_queries"] = {}

    segment_table_columns = set(segment_table_dict['columns'].split("-"))
    segment_table_columns = list(segment_table_columns)
    select_clause = ', '.join([f"id.{x}" for x in segment_table_columns])
    not_null_clause = " and ".join([f"id.{x} is not NULL" for x in segment_table_columns])
    limit_clause = SEGMENT_QUERY_LIMIT_CLAUSE

    identifies_table = segment_table
    ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
    persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
    watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"

    #------------------------------------------------------------
    query_metadata = {}
    segment_query_name = "user_id_query"
    query = f"\
        select distinct {select_clause}, ei.uuid as user_id_uuid\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.user_id = ei.uuid\
        where {not_null_clause} {limit_clause}"

    query_metadata["sql"] = query = clean_query(query)
    columns = [*segment_table_columns, 'USER_ID_UUID']
    df = execute_batched_select_query(
        query, columns, 
        batch_size=SEGMENT_QUERY_BATCH_SIZE, 
        timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
        conn=conn, verbose=verbose)
    query_metadata["data_frame"] = df
    metadata_table_dict["segment_queries"][segment_query_name] = query_metadata

    #------------------------------------------------------------
    query_metadata = {}
    segment_query_name = "username_query"
    query = f"\
        select distinct {select_clause}, ei.uuid as username_uuid\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.email = ei.username\
        where {not_null_clause} {limit_clause}"
    
    query_metadata["sql"] = query = clean_query(query) 
    columns = [*segment_table_columns, 'USERNAME_UUID']
    df = execute_batched_select_query(
        query, columns, 
        batch_size=SEGMENT_QUERY_BATCH_SIZE, 
        timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
        conn=conn, verbose=verbose)
    query_metadata["data_frame"] = df
    metadata_table_dict["segment_queries"][segment_query_name] = query_metadata

    #------------------------------------------------------------
    query_metadata = {}
    segment_query_name = "persona_query"
    query = f"\
        select distinct {select_clause}, ei.uuid as persona_uuid\
        from {identifies_table} id\
        join {persona_users_table} pu\
            on id.user_id = pu.id  \
        join {ellis_island_table} ei\
            on pu.id = ei.uuid\
        where {not_null_clause} {limit_clause}"
    
    query_metadata["sql"] = query = clean_query(query)
    columns = [*segment_table_columns, 'PERSONA_UUID']
    df = execute_batched_select_query(
        query, columns, 
        batch_size=SEGMENT_QUERY_BATCH_SIZE, 
        timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
        conn=conn, verbose=verbose)
    query_metadata["data_frame"] = df
    metadata_table_dict["segment_queries"][segment_query_name] = query_metadata

    # if identifies_table has column 'RID'
    if "RID" in segment_table_columns:
        #------------------------------------------------------------
        query_metadata = {}
        segment_query_name = "rid_query"
        query = f"\
         select distinct {select_clause}, ei.uuid as rid_uuid\
            from {identifies_table} id\
            join {watchtime_table} wt\
                on id.rid = wt.rid\
            join {ellis_island_table} ei\
                on wt.user_id = ei.uuid\
            where {not_null_clause} and id.rid is not NULL {limit_clause}"
              
        query_metadata["sql"] = query = clean_query(query)
        columns = [*segment_table_columns, 'RID_UUID']
        df = execute_batched_select_query(
            query, columns, 
            batch_size=SEGMENT_QUERY_BATCH_SIZE, 
            timeout_seconds=SEGMENT_QUERY_TIMEOUT_SECONDS, 
            conn=conn, verbose=verbose)
        query_metadata["data_frame"] = df
        metadata_table_dict["segment_queries"][segment_query_name] = query_metadata
    
    return metadata_table_dict


# Given a metadata_table_dict, saves a timestamped json_metadata_file that includes 
# segment_name, segment_queries, as well as the timestamped data_file_name of the data_frame 
# that has been saved to local disk
def save_metadata_table_dict(metadata_table_dict: Dict[str,Any], verbose: bool=True) -> None:
    segment_table = metadata_table_dict['segment_table']
    metadata_table = get_metadata_table_from_segment_table(segment_table)
    metadata_table = f"{SEGMENT_METADATA}.{metadata_table}"

    # save the data_frame to a data_file_name for each segment_query_name
    # save data_frame for later restore
    # clear the data_frame propery before pprint
    
    saved_data_frames = {}
    for segment_query_name in SEGMENT_QUERY_NAMES:
        if segment_query_name in metadata_table_dict['segment_queries']:
            query_metadata = metadata_table_dict['segment_queries'][segment_query_name]
            data_file_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_{segment_query_name}_df"
            saved_data_frames[segment_query_name] = df = query_metadata["data_frame"]
            data_file_name = save_data_frame(data_file_base_name, df, format=PARQUET_FORMAT)
            query_metadata["data_file_name"] = data_file_name
            query_metadata["data_frame"] = None

    # dump the metadata_table_dict to a local json file
    
    metadata_table_dict_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_metadata_table_dict"
    metadata_table_dict_json_file_name = f"/tmp/{metadata_table_dict_base_name}-{datetime.datetime.utcnow().isoformat()}.json"
    with open(metadata_table_dict_json_file_name, 'w') as f:
        json.dump(metadata_table_dict, f)
    
    # restore data_frame property for each segment_query_name after the pprint
    
    for segment_query_name in SEGMENT_QUERY_NAMES:
        if segment_query_name in metadata_table_dict['segment_queries']:
            query_metadata = metadata_table_dict['segment_queries'][segment_query_name]
            query_metadata["data_frame"] = saved_data_frames[segment_query_name]

# Given a metadata_table_dict, loads the latest metadata_table_dict json files for the given segment_table_dict.
# The metadata_table_dict contains the data_file_name, from which each data_frame can be loaded 
def load_latest_metadata_table_dict(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Optional[Dict[str,Any]]:

    metadata_table_dict = None
    segment_table = segment_table_dict['segment_table']
    metadata_table = get_metadata_table_from_segment_table(segment_table)
    metadata_table = f"{SEGMENT_METADATA}.{metadata_table}"
    
    # attempt to load the metadata_table_dict from a local json file

    metadata_table_dict_base_name = f"{BATCH_SEGMENT_TABLE_METADATA}_{metadata_table}_metadata_table_dict"
    metadata_table_dict_json_file_name = find_latest_file(f"/tmp/{metadata_table_dict_base_name}-*.json")
    
    if metadata_table_dict_json_file_name is not None:

        with open(metadata_table_dict_json_file_name, 'r') as f:
            metadata_table_dict = json.load(f)

        # deserialize the data_frame for each segment_query_name
        for segment_query_name in SEGMENT_QUERY_NAMES:
            if segment_query_name in metadata_table_dict["segment_queries"]:
                query_metadata = metadata_table_dict["segment_queries"][segment_query_name]
                data_file_name = query_metadata["data_file_name"]
                df = query_metadata["data_frame"] = load_data_frame(data_file_name)
        
    return metadata_table_dict

    
# Loads the latest saved set of segment_tables, computes and saves data_frames of metadata for each segment_table to disk
def  compute_and_save_new_metadata_table_dicts_for_all_segment_table_dicts(conn: connector=None, verbose: bool=True) -> None:
    _, segment_tables_df = get_segment_table_dicts_df(load_latest=True, verbose=verbose)
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    total_segment_tables =  len(segment_table_dicts)
    print("total_segment_tables:", total_segment_tables)
    
    cnt = 1
    for segment_table_dict in segment_table_dicts:
        print()
        print(f"computing segment_table_metadata ({cnt} out of {total_segment_tables})")
        
        metadata_table_dict = compute_metadata_table_dict(segment_table_dict, conn=conn, verbose=verbose)
        print(f"saving segment_table_metadata ({cnt} out of {total_segment_tables})")
        save_metadata_table_dict(metadata_table_dict, verbose=verbose)
        
        if verbose:
            show_metadata_table_dict(metadata_table_dict, caller=f"compute_and_save_new_metadata_for_all_segment_tables() ({cnt} out of {total_segment_tables})")
            
        cnt += 1

# read the latest metadata_table_dict file for each segment_table
def  load_latest_metadata_table_dicts_for_all_segment_table_dicts(conn: connector=None, verbose: bool=True) -> None:
    _, segment_tables_df = get_segment_table_dicts_df(load_latest=True, verbose=verbose)
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    total_segment_tables =  len(segment_table_dicts)
    print("total_segment_tables:", total_segment_tables)

    cnt = 1
    for segment_table_dict in segment_table_dicts:
        print(f"loading latest segment_table_metadata ({cnt} out of {total_segment_tables})")
        metadata_table_dict = load_latest_metadata_table_dict(segment_table_dict, conn=conn, verbose=verbose)
        
        if verbose:
            show_metadata_table_dict(metadata_table_dict, caller=f"load_latest_metadata_for_all_segment_tables() ({cnt} out of {total_segment_tables})")
            
        cnt += 1


################################################
# Tests
################################################

def test():
    conn = create_connector()
        
    print(" compute_and_save_new_metadata_table_dicts_for_all_segment_table_dicts")
    compute_and_save_new_metadata_table_dicts_for_all_segment_table_dicts(conn=conn, verbose=True)
    
    print("load_latest_metadata_for_all_segment_tables")
    load_latest_metadata_table_dicts_for_all_segment_table_dicts(conn=conn, verbose=True)

    print("all tests passed in", os.path.basename(__file__))

def main():
    test()

if __name__ == "__main__":
    main()
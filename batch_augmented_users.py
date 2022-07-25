from typing import Dict, List, Any
from constants import *
import snowflake.connector as connector
from query_generator import create_connector, execute_batched_select_query, execute_count_query, clean_query
import pandas as pd
from segment_tables import get_segment_tables_df, get_segment_table_dicts
import datetime
import pprint


def test_execute_count_query(select_query: str, conn: connector=None, verbose: bool=True):
    count_query = select_query.replace("select distinct", "select count(distinct")
    count_query = count_query.replace(" from ", ") from ")
    count_query = count_query.replace(" as valid_uuid","")
    count_query = " ".join(count_query.strip().split())
    count = execute_count_query(count_query, conn=conn, verbose=verbose)
    print(f"count:{count}")

# Returns segment_table_metadata as a list with the following dict structure
# [
#     { 
#         "segment_table": <segment_table>,
#         "queries": [
#             {"name":"rid", "sql": <select_str>, "dataframe": <pd.DataFrame>, "timestamp": <timestamp>},
#             {"name":"user_id", "sql": <select_str>, "dataframe": <pd.DataFrame>, "timestamp": <timestamp>},
#             {"name":"username", "sql": <select_str>, "dataframe": <pd.DataFrame>, "timestamp": <timestamp>},
#             {"name":"persona", "sql": <select_str>, "dataframe": <pd.DataFrame>, "timestamp": <timestamp>},
#         ]
#     },
# ]
#
# Given a segment_table_dict with the following structure:
#  {'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
#  'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES'
# }

def compute_segment_table_metadata(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> List[Any]:
    batch_size = 1000

    segment_table = segment_table_dict['segment_table']
    segment_table_metadata = {}
    segment_table_metadata["segment_table"] = segment_table
    segment_table_metadata["queries"] = []

    segment_table_columns = set(segment_table_dict['columns'].split("-"))
    segment_table_columns = list(segment_table_columns)
    select_clause = ', '.join([f"id.{x}" for x in segment_table_columns]) + ', ei.uuid as valid_uuid'
    not_null_clause = " and ".join([f"id.{x}" for x in segment_table_columns]) + " and ei.uuid is not NULL"

    identifies_table = segment_table
    ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
    persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
    watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"
    
    # if identifies_table has column 'RID'
    if "RID" in segment_table_columns:
        rid_query = f"\
         select distinct {select_clause}, id.rid\
            from {identifies_table} id\
            join {watchtime_table} wt\
                on id.rid = wt.rid\
            join {ellis_island_table} ei\
                on wt.user_id = ei.uuid\
            where {not_null_clause} and id.rid is not NULL"
         
        if verbose:   
            test_execute_count_query(rid_query, conn=conn, verbose=verbose)
        
        
        rid_query_metadata = {}
        rid_query_metadata["name"] = "rid_query"
        rid_query_metadata["sql"] = clean_query(rid_query)
        
        uuid_timestamp = datetime.datetime.utcnow().isoformat()
        rid_df = execute_batched_select_query(rid_query, segment_table_columns, batch_size=batch_size, conn=conn)
        rid_df = rid_df.rename(columns={'valid_uuid':'rid_uuid'})
        rid_df['uuid_timestamp'] = uuid_timestamp
        rid_query_metadata["dataframe"] = rid_df
        rid_query_metadata["uuid_timestamp"] = uuid_timestamp
        
        segment_table_metadata["queries"].append(rid_query)

    user_id_query = f"\
        select distinct {select_clause}\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.user_id = ei.uuid\
        where {not_null_clause}"

    if verbose:   
        test_execute_count_query(user_id_query, conn=conn, verbose=verbose)
    
    user_id_query_metadata = {}
    user_id_query_metadata["name"] = "user_id_query"
    user_id_query_metadata["sql"] = clean_query(user_id_query)
    
    uuid_timestamp = datetime.datetime.utcnow().isoformat()
    user_id_df = execute_batched_select_query(user_id_query, segment_table_columns, batch_size=batch_size, conn=conn, verbose=verbose)
    user_id_df = user_id_df.rename(columns={'valid_uuid':'user_id_uuid'})
    user_id_df['uuid_timestamp'] = uuid_timestamp
    user_id_query_metadata["dataframe"] = user_id_df
    user_id_query_metadata["uuid_timestamp"] = uuid_timestamp
    
    segment_table_metadata["queries"].append(user_id_query_metadata)

    username_query = f"\
        select distinct {select_clause}\
        from {identifies_table} id\
        join {ellis_island_table} ei\
            on id.email = ei.username\
        where {not_null_clause}"
    
    if verbose:   
        test_execute_count_query(username_query, conn=conn, verbose=verbose)
    
    username_query_metadata = {}
    username_query_metadata["name"] = "username_query"
    username_query_metadata["sql"] = clean_query(username_query)

    uuid_timestamp = datetime.datetime.utcnow().isoformat()
    username_df = execute_batched_select_query(username_query, segment_table_columns, batch_size=batch_size, conn=conn)
    username_df = username_df.rename(columns={'valid_uuid':'username_uuid'})
    username_df['uuid_timestamp'] = uuid_timestamp
    username_query_metadata["dataframe"] = username_df
    username_query_metadata["uuid_timestamp"] = uuid_timestamp
    
    segment_table_metadata["queries"].append(username_query_metadata)

    persona_query = f"\
        select distinct {select_clause}\
        from {identifies_table} id\
        join {persona_users_table} pu\
            on id.user_id = pu.id  \
        join {ellis_island_table} ei\
            on pu.id = ei.uuid\
        where {not_null_clause}"
    
    if verbose:   
        test_execute_count_query(persona_query, conn=conn, verbose=verbose)

    persona_query_metadata = {}
    persona_query_metadata["name"] = "persona_query"
    persona_query_metadata["sql"] = clean_query(persona_query)

    uuid_timestamp = datetime.datetime.utcnow().isoformat()
    persona_df = execute_batched_select_query(persona_query, segment_table_columns, batch_size=batch_size, conn=conn)
    persona_df = persona_df.rename(columns={'valid_uuid':'persona_uuid'})
    persona_df['uuid_timestamp'] = uuid_timestamp
    persona_query_metadata["dataframe"] = persona_df
    persona_query_metadata["uuid_timestamp"] = uuid_timestamp
    
    segment_table_metadata["queries"].append(persona_query_metadata)
        
    return segment_table_metadata


# Prints all query metadata for the given segment_table
def output_segment_table_metadata(segment_table_metadata) -> None:
    segment_metadata = "SEGMENT.IDENTIFIES_METADATA"

    segment_table = segment_table_metadata['segment_table']
    for i in range(len(segment_table_metadata['queries'])):
        query = segment_table_metadata['queries'][i]
        name = query['name']
        df = query['dataframe']
        uuid_timestamp = query['uuid_timestamp']
        view = {
            "metadata": f"{segment_metadata}.{segment_table}",
            "name": name,
            "timestamp": uuid_timestamp,
            "shape":df.shape,
            "columns": df.columns
        }
        pprint.pprint(view)
    
    
# Loads the latest saved set of segment_tables and then computes and outputs metadata for each to the segment_metadata_table
def compute_and_save_metadata_for_all_segment_tables(conn: connector=None, verbose: bool=True) -> None:
    csv_file, segment_tables_df = get_segment_tables_df(load_latest=True, verbose=verbose)
    
    # converts 2-column dataframe to a list of 2-property dicts
    segment_table_dicts = get_segment_table_dicts(segment_tables_df)
    if verbose:
        print("num segment_table_dicts:", len(segment_table_dicts))
    
    for segment_table_dict in segment_table_dicts:
        segment_table_metadata = compute_segment_table_metadata(segment_table_dict, conn=conn, verbose=verbose)
        output_segment_table_metadata(segment_table_metadata)


    
################################################
# Tests
################################################

def test():
    conn = create_connector()
        
    print("compute_segment_table_metadata")
    compute_and_save_metadata_for_all_segment_tables(conn=conn, verbose=True)

def main():
    test()

if __name__ == "__main__":
    main()
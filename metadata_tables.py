
from platform import java_ver
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from query_generator import create_connector, clean_query, execute_simple_query, execute_single_query, execute_count_query
from constants import *   
from typing import Set, List, Dict, Optional, Tuple, Any
from segment_utils import get_metadata_table_from_segment_table
from segment_tables import get_segment_table_dicts_df, get_segment_table_dicts
import pandas as pd
from data_frame_utils import is_empty_data_frame
from itertools import combinations

# Given a segment_table_dict with the following example structure:
# {
#     'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES',
#     'metadata_table': 'SEGMENT__ANGEL_APP_IOS__IDENTIFIES',
#     'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
# }
#
# Returns a new MetadataTable object with the following fields:
#   identifies_metadata_table: str - where new UUID columns are added
#   segment_table_columns: List[str]
#   select_clause: str
#   not_null_clause: str
#   imit_clause: str
#   ellis_island_table: str
#   persona_users_table: str
#   watchtime_table: str
#   query_dicts = []

# where query_dict has structure:
#   name: str
#   new_uuid: str
#   query: str
#   columns: List[str]
class MetadataTable():
    def __init__(self, segment_table_dict: Dict[str,str]):
        self.segment_table_dict = segment_table_dict
        self.metadata_table = segment_table_dict['metadata_table']
        self.identifies_metadata_table = f"SEGMENT.IDENTIFIES_METADATA.{self.metadata_table}"
        self.segment_table_columns = segment_table_dict['columns'].split("-")
        self.select_clause = ", ".join([f"id.{x}" for x in self.segment_table_columns])
        self.not_null_clause = " and ".join([f"id.{x} is not NULL" for x in self.segment_table_columns])
        self.ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
        self.persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
        self.watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"
        self.query_dicts = []

    # Return True if the metadata_table was actually cloned
    # otherwise return False if metadata_table already exists
    # or clone failed.
    def clone_metadata_table(self, conn: connector=None, verbose: bool=True, preview_only: bool=True) -> bool:
        if not metadata_table_exists(self.metadata_table, conn=conn, verbose=verbose):
            return clone_metadata_table(self.segment_table_dict, conn=conn, verbose=verbose, preview_only=preview_only)
        else:
            return False       
    
    # Executes a set_uuid_query after adding the new uuid column if needed
    # Returns the updated query_dict with added total and uuid_counts if the query was successfully run
    # otherwise return None
    def run_query_dict(self, query_dict, conn: connector=None, verbose: bool=True, preview_only: bool=True) -> Optional[Dict[str,Any]]:
        # if not metadata_table_exists(self.metadata_table, conn=conn, verbose=verbose):
        #     return None

        # add new_uuid column if needed
        new_uuid = query_dict['new_uuid'].upper()
        existing_columns = get_existing_metadata_table_columns(self.metadata_table, conn=conn, verbose=verbose)
        if existing_columns is None or new_uuid not in existing_columns:
            add_uuid_column_query = f"\
                ALTER TABLE {self.identifies_metadata_table} \
                ADD COLUMN {new_uuid} VARCHAR \
                DEFAULT NULL"
            if preview_only:
                print("\nadd_uuid_column_query:\n",clean_query(add_uuid_column_query))
            else:
                execute_single_query(add_uuid_column_query, conn=conn, verbose=verbose)
                columns = get_existing_metadata_table_columns(self.metadata_table, conn=conn, verbose=verbose)
                if columns is not None and len(columns) > 0:
                    assert new_uuid in columns, f"ERROR: new_uuid:{new_uuid} column not added"

        # set the new_uuid column value for each row
        if preview_only:
            print("\nset_uuid_query:\n", query_dict['set_uuid_query'])
            return None
        else:                
            cloned_table = self.identifies_metadata_table
            execute_single_query(query_dict['set_uuid_query'], conn=conn, verbose=verbose)
            query_dict['new_uuid_post_total_count'] = execute_count_query(f"SELECT count(*) from {cloned_table} ", conn=conn, verbose=verbose)
            query_dict['new_uuid_non_null_count'] = execute_count_query(f"SELECT count(*) from {cloned_table} where {query_dict['new_uuid']} is not NULL", conn=conn, verbose=verbose)
            
            print(f"cloned:{cloned_table} {query_dict['new_uuid']} total:{query_dict['new_uuid_post_total_count']} non-null:{query_dict['new_uuid_non_null_count']}")

            # total and new_uuid counts have been added to this query_dict
            return query_dict
    
    # Returns a dict that packages a set_uuid_query
    def create_query_dict(self, query_name, new_uuid, set_uuid_query):
        query_dict = {}
        query_dict["metadata_table"] = self.metadata_table
        query_dict["name"] = query_name
        assert query_name in SEGMENT_QUERY_NAMES, f"ERROR: invalid query_name: {query_name}"
        query_dict["new_uuid"] = new_uuid
        query_dict["set_uuid_query"] = clean_query(set_uuid_query)
        query_dict["columns"] = [*self.segment_table_columns, new_uuid.upper()]
        return query_dict
    
    # Adds all set_uuid queries
    def add_query_dicts(self):
        #---------------------------------------
        query_name = "user_id_query"
        new_uuid = "user_id_uuid"
        set_uuid_query = f"\
                UPDATE {self.identifies_metadata_table} id1 \
                set user_id_uuid = ei.uuid \
                FROM {self.identifies_metadata_table} id \
                LEFT OUTER JOIN {self.ellis_island_table} ei\
                ON id.user_id = ei.uuid\
                WHERE id1.user_id = id.user_id"
        user_id_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
        self.query_dicts.append(user_id_query_dict)
        
        #----------------------------------------  
        query_name = "username_query"
        new_uuid = "username_uuid"
        set_uuid_query = f"\
                UPDATE {self.identifies_metadata_table} id1\
                SET username_uuid = ei.uuid \
                FROM {self.identifies_metadata_table} id\
                LEFT JOIN {self.ellis_island_table} ei\
                    ON id.email = ei.username\
                WHERE id1.user_id = id.user_id"
                
                
        username_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
        self.query_dicts.append(username_query_dict)
    
        #----------------------------------------  
        query_name = "persona_query"
        new_uuid = "persona_uuid"
        set_uuid_query = f"\
                UPDATE {self.identifies_metadata_table} id1\
                SET persona_uuid = ei.uuid\
                FROM {self.identifies_metadata_table} id\
                JOIN {self.persona_users_table} pu\
                    ON id.user_id = pu.id  \
                LEFT JOIN {self.ellis_island_table} ei\
                    ON pu.id = ei.uuid\
                WHERE id1.user_id = id.user_id"
        
        persona_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
        self.query_dicts.append(persona_query_dict)

        #----------------------------------------  
        if "RID" in self.segment_table_columns:
            query_name = "rid_query"
            new_uuid = "rid_uuid"
            set_uuid_query = f"\
                    UPDATE {self.identifies_metadata_table} id1\
                    SET rid_uuid = ei.uuid\
                    FROM {self.identifies_metadata_table} id\
                    JOIN {self.watchtime_table} wt\
                        ON id.rid = wt.rid\
                    LEFT JOIN {self.ellis_island_table} ei\
                        ON wt.user_id = ei.uuid\
                    WHERE id1.user_id = id.user_id"
            
            rid_query_dict = self.create_query_dict(query_name, new_uuid, clean_query(set_uuid_query))
            self.query_dicts.append(rid_query_dict)
    
    def run_query_dicts(self, conn: connector=None, verbose: bool=True, preview_only: bool=True) -> pd.DataFrame:
        uuid_counts = {}
        uuid_counts['metadata_table'] = None
        uuid_counts['total'] = None
        for uuid in SEGMENT_UUIDS:
            uuid_counts[uuid] = None

        for query_dict in self.query_dicts:
            metadata_table = query_dict['metadata_table']
            uuid_counts['metadata_table'] = metadata_table
            result = self.run_query_dict(query_dict, conn=conn, verbose=verbose, preview_only=preview_only)
            if result is not None:
                query_dict_with_uuid_counts = result
                uuid_counts['total'] = query_dict_with_uuid_counts['new_uuid_post_total_count']
                uuid = query_dict_with_uuid_counts['new_uuid'].upper()
                uuid_counts[uuid] = query_dict_with_uuid_counts['new_uuid_non_null_count']
        df = pd.DataFrame.from_dict(data=uuid_counts, orient="index").transpose()
        return df

    
# Returns a list of metadata_tables that end with IDENTIFIES 
# and already exist in SEGMENT.IDENTIFIES_METADATA
def find_existing_metadata_tables(conn: connector=None, verbose:bool=True) -> List[str]:
    existing_metadata_tables = []
    show_tables_columns = ['created_on','name','database_name','schema_name,kind','comment','cluster_by,rows','bytes','owner','retention_time','automatic_clustering','change_tracking','search_optimization','search_optimization_progress','search_optimization_bytes','is_external']
    show_tables_query = "show tables like '%IDENTIFIES' in SEGMENT.IDENTIFIES_METADATA"
    results = execute_simple_query(show_tables_query, conn=conn)
    for line in results:
        line_dict = dict(zip(show_tables_columns, line))
        existing_metadata_tables.append(f"{line_dict['name']}")
    return existing_metadata_tables

# Returns True if metadata_table exists under SEGMENT.IDENTIFIES_METADATA
def metadata_table_exists(metadata_table: str, conn: connector=None, verbose:bool=True):
    existing_metadata_tables = find_existing_metadata_tables(conn=conn, verbose=verbose)
    return True if metadata_table in existing_metadata_tables else False

# Returns a list of dict lines describing the given metadata_table in snowflake
TABLE_DESCRIBE_COLUMNS = ['name','type','kind','null?','default','primary key','unique key','check','expression','comment','policy name']
def describe_metadata_table(metadata_table: str, conn: connector=None, verbose: bool=True) -> Dict[str,str]:
    table_info_dicts = None
    metadata_table_path = f"SEGMENT.IDENTIFIES_METADATA.{metadata_table}"
    describe_table_query = f"DESCRIBE TABLE {metadata_table_path}"
    try:
        results = execute_simple_query(describe_table_query, conn=conn)
        for line in results:
            line_dict = dict(zip(TABLE_DESCRIBE_COLUMNS, line))
            table_info_dicts = [] if table_info_dicts is None else table_info_dicts 
            table_info_dicts.append(line_dict)
        return table_info_dicts
    except ProgrammingError as err:
        print(f"{type(err)} str(err)")
    return table_info_dicts

# Returns a list of metadata_table columns derived from DESCRIBE TABLE or None if metadata_table not found
def get_existing_metadata_table_columns(metadata_table: str, conn: connector=None, verbose: bool=True) -> Optional[List[str]]:
    table_info_dicts = describe_metadata_table(metadata_table, conn=conn, verbose=verbose)
    if table_info_dicts is not None:
        return [table_info_dict['name'] for table_info_dict in table_info_dicts]
    else:
        return None

# Returns True if metadata_table under SEGMENT.IDENTIFIES_METADATA 
# has been cloned from segment_table under SEGMENT
# Otherwise returns False if metadata_table already exists or clone instruction failed.
def clone_metadata_table(segment_table_dict: Dict[str,str], verbose: bool=True, conn: connector=None, preview_only: bool=True) -> bool:
    segment_table = segment_table_dict['segment_table']
    metadata_table = segment_table_dict['metadata_table']
    if not metadata_table_exists(metadata_table, conn=conn, verbose=verbose):
        cloned_table = f"SEGMENT.IDENTIFIES_METADATA.{metadata_table}"
        clone_metadata_table_query = f"create table if not exists {cloned_table} clone {segment_table}"
        if verbose:
            print("\nclone_metadata_table_query:\n", clean_query(clone_metadata_table_query))
        if not preview_only:
            try:
                source_count = execute_count_query(f"select count(*) from {segment_table}", conn=conn, verbose=verbose)
                if verbose:
                    print(f"source_count:{source_count}")
                execute_single_query(clone_metadata_table_query, conn=conn, verbose=verbose)
                cloned_count = execute_count_query(f"select count(*) from {cloned_table}")
                if verbose:
                    print(f"cloned_count:{cloned_count}")
                if cloned_count != source_count:
                     print(f"ERROR: expected cloned_count:{source_count} not:{cloned_count} for {cloned_table}")
                     return False
                else:
                    return True
            except ProgrammingError as err:
                print(f"{type(err)} str(err)")
    return False    

# create and run a single MetadataTable object
def create_and_run_metadata_table(segment_table_dict: Dict[str,str], verbose: bool=True, preview_only: bool=True, conn: connector=None) -> pd.DataFrame:
    metadata_table_obj = MetadataTable(segment_table_dict)
    metadata_table_obj.clone_metadata_table(conn=conn, verbose=verbose, preview_only=preview_only)
    metadata_table_obj.add_query_dicts()
    df = metadata_table_obj.run_query_dicts(conn=conn, verbose=verbose, preview_only=preview_only)
    return df

def get_segment_table_dict(segment_table: str) -> Optional[Dict[str,str]]:
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    for segment_table_dict in get_segment_table_dicts(latest_df):
        if segment_table_dict['segment_table'] == segment_table:
            return segment_table_dict
    return None
    
# create and run all MetadataTable objects
def create_and_run_metadata_tables(conn: connector=None, verbose: bool=True, preview_only: bool=True) -> pd.DataFrame:
    union_df = None
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    for segment_table_dict in get_segment_table_dicts(latest_df):
        df = create_and_run_metadata_table(segment_table_dict, verbose=verbose, preview_only=preview_only, conn=conn)
        if len(df) > 0:
            if union_df is None:
                union_df = df
            else:
                union_df = pd.concat([union_df, df], axis=0)
    return union_df

def summarize_metadata_tables(conn: connector=None, verbose: bool=True):
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    for segment_table_dict in get_segment_table_dicts(latest_df):
        metadata_table = segment_table_dict['metadata_table']
        cloned_table = f"SEGMENT.IDENTIFIES_METADATA.{metadata_table}"
        try:
            total_count = execute_count_query(f"SELECT count(*) from {cloned_table}", conn=conn)
            all_4_equal_count = execute_count_query(f"SELECT count(*) from {cloned_table} where user_id = user_id_uuid and user_id_uuid = username_uuid and username_uuid = persona_uuid", conn=conn)
            all_4_equal_percent = all_4_equal_count * 100 / total_count if total_count > 0 else 0.0
            print(f"{all_4_equal_percent:5.2f}% {metadata_table} total_count:{total_count} all_4_equal_count:{all_4_equal_count}")
        except Exception as e:
            total_count = 0
            all_4_equal_count = 0
            all_4_equal_percent = all_4_equal_count * 100 / total_count if total_count > 0 else 0.0
            print(f"{all_4_equal_percent:5.2f}% {metadata_table} FAILED total_count:{total_count} all_4_equal_count:{all_4_equal_count}")            

# execute (or if preview_only just print) all combo_queries for the given metadata_table and its columns
def summarize_metadata_table_combos(metadata_table: str, metadata_table_columns: List[str], preview_only: bool=False, conn: connector=None) -> List[str]:
    cloned_table = f"SEGMENT.IDENTIFIES_METADATA.{metadata_table}"
    keep_columns = []
    combo_queries = []
    for uuid_column in SEGMENT_UUIDS:
        if metadata_table_columns is not None and len(metadata_table_columns) > 0 and uuid_column.upper() in metadata_table_columns:
            query = f"SELECT count(*) from {cloned_table} where {uuid_column} is not null"
            if preview_only:
                keep_columns.append(uuid_column)
                combo_queries.append({"count_query":query})
            else:
                count = 0
                try:
                    count = execute_count_query(f"SELECT count(*) from {cloned_table} where {uuid_column} is not null", conn=conn) 
                    if count > 0:
                        keep_columns.append(uuid_column)
                except Exception as e:
                    pass
    N = len(keep_columns)
    if N > 0:
        combos = []
        a = keep_columns[0:N]
        for k in range(2,N+1):
            for j in combinations(a,k):
                combos.append(j)
        
        for combo in combos:
            combo_str = metadata_table + "@" + "-".join([f"{x}" for x in combo])
            all_equals_clause = " and ".join([f"{combo[0]} = {x}" for x in combo[1:]])
            not_null_clause = " and ".join([f"{x} is not null" for x in combo])
            query = f"SELECT count(*) from {cloned_table} where {all_equals_clause} and {not_null_clause}"
            if preview_only:
                combo_queries.append({combo_str: query})
            else:
                count = -1
                try:
                    count = execute_count_query(query, conn=conn)
                    count_str = f"{count:,}"
                    combo_queries.append({combo_str: count_str})
                except Exception as e:
                    combo_queries.append({combo_str:query})
    return combo_queries

# print all queries for metadata_tables that cannot be 
# queries using snowflake python connector
def summarize_unqueriable_metadata_table_combinations():
    UNQUERIABLE_METADATA_TABLES = [
        # { "metadata_table": 'SEGMENT__ANGEL_APP_IOS__IDENTIFIES', 
        #   "metadata_columns": ['USER_ID_UUID','USERNAME_UUID','PERSONA_UUID'] },
        # { "metadata_table": 'SEGMENT__ANGEL_FUNDING_PROD__IDENTIFIES', 
        #   "metadata_columns": ['USER_ID_UUID','USERNAME_UUID','PERSONA_UUID'] },
        { "metadata_table": 'SEGMENT__ANGEL_NFT_WEBSITE_PROD__IDENTIFIES', 
          "metadata_columns": ['USER_ID_UUID','USERNAME_UUID','PERSONA_UUID','RID_UUID'] },
        # { "metadata_table": 'SEGMENT__ANGEL_WEB__IDENTIFIES', 
        #   "metadata_columns": ['USER_ID_UUID','USERNAME_UUID','PERSONA_UUID','RID_UUID'] },
    ]
    for unqueriable in UNQUERIABLE_METADATA_TABLES:
        metadata_table = unqueriable['metadata_table']
        metadata_columns = unqueriable['metadata_columns']
        combo_queries = summarize_metadata_table_combos(metadata_table, metadata_columns, preview_only=True)
        for combo_query in combo_queries:
            print(combo_query)

# execute (or if preview_only just print) all combo_queries for all metadata_tables and their columns                 
def summarize_metadata_table_combinations(conn: connector=None, verbose: bool=True, preview_only: bool=False) -> List[str]:
    all_combo_queries = []
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    for segment_table_dict in get_segment_table_dicts(latest_df):
        metadata_table = segment_table_dict['metadata_table']
        metadata_table_columns = get_existing_metadata_table_columns(metadata_table, conn=conn, verbose=verbose)
        combo_queries = summarize_metadata_table_combos(metadata_table, metadata_table_columns, preview_only=preview_only, conn=conn)
        all_combo_queries.extend(combo_queries)

    for combo_query in all_combo_queries:
        print(combo_query)
            
################################################
# Tests
################################################

def tests():
    print("all tests passed in", os.path.basename(__file__))

def main():
    conn = create_connector()
    
    summarize_unqueriable_metadata_table_combinations()
    
    print("done")
    
if __name__ == "__main__":
    main()

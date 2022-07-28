
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from query_generator import clean_query, execute_simple_query, execute_single_query, execute_count_query
from constants import *   
from typing import Set, List, Dict
from segment_utils import get_metadata_table_from_segment_table
from segment_tables import get_segment_table_dicts_df

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
        
        self.identifies_metadata_table = f"SEGMENT.IDENTIFIES_METADATA.{segment_table_dict['metadata_table']}"
        self.segment_table_columns = list(set(segment_table_dict['columns'].split("-")))
        self.select_clause = ', '.join([f"id.{x}" for x in self.segment_table_columns])
        self.not_null_clause = " and ".join([f"id.{x} is not NULL" for x in self.segment_table_columns])
        self.limit_clause = SEGMENT_QUERY_LIMIT_CLAUSE
        self.ellis_island_table = "STITCH_LANDING.ELLIS_ISLAND.USER"
        self.persona_users_table = "SEGMENT.PERSONAS_THE_CHOSEN_WEB.USERS"
        self.watchtime_table = "STITCH_LANDING.CHOSENHYDRA.WATCHTIME"
        self.query_dicts = []
        
    def create_query_dict(self, query_name, new_uuid, query):
        query_dict = {}
        query_dict["name"] = query_name
        assert query_name in SEGMENT_QUERY_NAMES, f"ERROR: invalid query_name: {query_name}"
        query_dict["new_uuid"] = new_uuid
        query_dict["query"] = clean_query(query)
        query_dict["columns"] = [*self.segment_table_columns, new_uuid.upper()]
        return query_dict
    
    def add_query_dicts(self):
        #---------------------------------------
        query_name = "user_id_query"
        new_uuid = "user_id_uuid"
        query = f"\
            select distinct {self.select_clause}, ei.uuid as {new_uuid}\
            from {self.identifies_metadata_table} id\
            left join {self.ellis_island_table} ei\
                on id.user_id = ei.uuid\
            where {self.not_null_clause} {self.limit_clause}"
        user_id_query_dict = self.create_query_dict(query_name, new_uuid, query)
        self.query_dicts.append(user_id_query_dict)
        
        #----------------------------------------  
        query_name = "username_query"
        new_uuid = "usename_uuid"
        query = f"\
            select distinct {self.select_clause}, ei.uuid as username_uuid\
            from {self.identifies_metadata_table} id\
            left join {self.ellis_island_table} ei\
                on id.email = ei.username\
            where {self.not_null_clause} {self.limit_clause}"
        username_query_dict = self.create_query_dict(query_name, new_uuid, query)
        self.query_dicts.append(username_query_dict)
    
        #----------------------------------------  
        query_name = "persona_query"
        new_uuid = "persona_uuid"
        query = f"\
            select distinct {self.select_clause}, ei.uuid as persona_uuid\
            from {self.identifies_metadata_table} id\
            join {self.persona_users_table} pu\
                on id.user_id = pu.id  \
            left join {self.ellis_island_table} ei\
                on pu.id = ei.uuid\
            where {self.not_null_clause} {self.limit_clause}"
        persona_query_dict = self.create_query_dict(query_name, new_uuid, query)
        self.query_dicts.append(persona_query_dict)

        #----------------------------------------  
        if "RID" in self.segment_table_columns:
            query_name = "rid_query"
            new_uuid = "rid_uuid"
            query = f"\
                select distinct {self.select_clause}, ei.uuid as rid_uuid\
                    from {self.identifies_metadata_table} id\
                    join {self.watchtime_table} wt\
                        on id.rid = wt.rid\
                    left join {self.ellis_island_table} ei\
                        on wt.user_id = ei.uuid\
                    where {self.not_null_clause} and id.rid is not NULL {self.limit_clause}"
            rid_query_dict = self.create_query_dict(query_name, new_uuid, query)
            self.query_dicts.append(rid_query_dict)

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

# Creates a clone in SEGMENT.IDENTIFIES_METADATA for for each 
# segment_table in SEGMENT described in segment_tables_df.
# Returns a list of the metadata_tables in SEGMENT.IDENTIFIES_METADATA 
# that have been newly cloned from source segment_tables in SEGMENT
def clone_metadata_tables(segment_tables_df, conn: connector=None, verbose:bool=True, preview_only: bool=True) -> List[str]:
    existing_metadata_tables = find_existing_metadata_tables(conn=conn)
    newly_cloned_tables = []
    for row in segment_tables_df.values:
        segment_table = row[0]
        metadata_table = get_metadata_table_from_segment_table(segment_table)
        if metadata_table not in existing_metadata_tables:
            cloned_table = f"SEGMENT.IDENTIFIES_METADATA.{metadata_table}"
            clone_query = f"create table if not exists {cloned_table} clone {segment_table}"
            if verbose:
                print(clone_query)
            if not preview_only:
                try:
                    source_count = execute_count_query(f"select count(*) from {segment_table}")
                    if verbose:
                        print(f"source_count:{source_count}")
                    execute_single_query(clone_query, conn=conn, verbose=verbose)
                    cloned_count = execute_count_query(f"select count(*) from {cloned_table}")
                    if verbose:
                            print(f"cloned_count:{cloned_count}")
                    assert cloned_count == source_count, f"ERROR: expected cloned_count:{source_count} but got {cloned_count}"
                    newly_cloned_tables.append(metadata_table)
                except Exception as e:
                    print(f"{type(e)} {str(e)}")
    if verbose:
        print("newly_cloned_tables:\n", newly_cloned_tables)
    return newly_cloned_tables

# Returns a list of metadata_tables that have not been
# cloned into SEGMENT.IDENTIFIES_METADATA from SEGMENT
def find_uncloned_metadata_tables(segment_tables_df, conn: connector=None, verbose:bool=True) -> List[str]:
    existing_metadata_tables = find_existing_metadata_tables(conn=conn)
    required_segment_tables = [row[0] for row in segment_tables_df.values]
    required_metadata_tables = [get_metadata_table_from_segment_table(x) for x in required_segment_tables ]
    uncloned_metadata_tables = list(set(required_metadata_tables) - set(existing_metadata_tables))
    return uncloned_metadata_tables

################################################
# Tests
################################################

def test_clone_latest_metadata_tables():
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    clone_metadata_tables(latest_df, preview_only=False)

def test_find_uncloned_metadata_tables():
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    uncloned_metadata_tables = find_uncloned_metadata_tables(latest_df)
    print(f"uncloned_metadata_tables: {len(uncloned_metadata_tables)}")
    for metadata_table in uncloned_metadata_tables:
        print(f"  uncloned_metadata_table: {metadata_table}")
        
def tests():
    test_clone_latest_metadata_tables()
    test_find_uncloned_metadata_tables()
    
    print()
    print("all tests passed in", os.path.basename(__file__))

def main():
    tests()
    
if __name__ == "__main__":
    main()

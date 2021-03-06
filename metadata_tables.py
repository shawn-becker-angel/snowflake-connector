
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from query_generator import create_connector, clean_query, execute_simple_query, execute_single_query, execute_count_query
from constants import *   
from typing import Set, List, Dict, Optional, Tuple
from segment_utils import get_metadata_table_from_segment_table
from segment_tables import get_segment_table_dicts_df, get_segment_table_dicts

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
        self.segment_table_columns = list(set(segment_table_dict['columns'].split("-")))
        self.select_clause = ', '.join([f"id.{x}" for x in self.segment_table_columns])
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
    # Returns True if the query was successfully run
    # otherwise return False
    def run_query_dict(self, query_dict, conn: connector=None, verbose: bool=True, preview_only: bool=True) -> bool:
        if not metadata_table_exists(self.metadata_table, conn=conn, verbose=verbose):
            return False

        # add new_uuid column if needed
        new_uuid = query_dict['new_uuid']
        if new_uuid not in get_existing_metadata_table_columns(self.metadata_table, conn=conn, verbose=verbose):
            add_uuid_column_query = f"\
                ALTER TABLE {self.identifies_metadata_table} \
                ADD COLUMN {query_dict['new_uuid']} VARCHAR \
                DEFAULT NULL"
            if preview_only:
                print("\nadd_uuid_column_query:\n",clean_query(add_uuid_column_query))
            else:
                execute_single_query(add_uuid_column_query, conn=conn, verbose=verbose)
                assert new_uuid in get_existing_metadata_table_columns(self.metadata_table, conn=conn, verbose=verbose), f"ERROR: new_uuid:{new_uuid} column not added"

        # set the new_uuid column value for each row
        if preview_only:
            print("\nset_uuid_query:\n", query_dict['set_uuid_query'])
            return False
        else:
            execute_single_query(query_dict['set_uuid_query'], conn=conn, verbose=verbose)
            return True

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
            WITH W AS (\
                SELECT {self.select_clause}, ei.uuid AS user_id_uuid\
                FROM {self.identifies_metadata_table} id\
                LEFT JOIN {self.ellis_island_table} ei\
                    ON id.user_id = ei.uuid\
                WHERE {self.not_null_clause}\
            ) UPDATE {self.identifies_metadata_table} id\
                SET id.user_id_uuid = W.user_id_uuid"

        user_id_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
        self.query_dicts.append(user_id_query_dict)
        
        #----------------------------------------  
        query_name = "username_query"
        new_uuid = "username_uuid"
        set_uuid_query = f"\
            WITH W AS (\
                SELECT {self.select_clause}, ei.uuid AS username_uuid\
                FROM {self.identifies_metadata_table} id\
                LEFT JOIN {self.ellis_island_table} ei\
                    ON id.email = ei.username\
                WHERE {self.not_null_clause}\
            ) UPDATE {self.identifies_metadata_table} id\
                SET id.username_uuid = W.username_uuid"
                
        username_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
        self.query_dicts.append(username_query_dict)
    
        #----------------------------------------  
        query_name = "persona_query"
        new_uuid = "persona_uuid"
        set_uuid_query = f"\
            WITH W AS (\
                SELECT {self.select_clause}, ei.uuid AS persona_uuid\
                FROM {self.identifies_metadata_table} id\
                JOIN {self.persona_users_table} pu\
                    ON id.user_id = pu.id  \
                LEFT JOIN {self.ellis_island_table} ei\
                    ON pu.id = ei.uuid\
                WHERE {self.not_null_clause}\
            ) UPDATE {self.identifies_metadata_table} id\
                SET id.persona_uuid = W.persona_uuid"
        
        persona_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
        self.query_dicts.append(persona_query_dict)

        #----------------------------------------  
        if "RID" in self.segment_table_columns:
            query_name = "rid_query"
            new_uuid = "rid_uuid"
            set_uuid_query = f"\
                WITH W AS (\
                    SELECT {self.select_clause}, ei.uuid AS rid_uuid\
                    FROM {self.identifies_metadata_table} id\
                    JOIN {self.watchtime_table} wt\
                        ON id.rid = wt.rid\
                    LEFT JOIN {self.ellis_island_table} ei\
                        ON wt.user_id = ei.uuid\
                    WHERE {self.not_null_clause}\
                ) UPDATE {self.identifies_metadata_table} id\
                    SET id.rid_uuid = W.rid_uuid"
            
            rid_query_dict = self.create_query_dict(query_name, new_uuid, set_uuid_query)
            self.query_dicts.append(rid_query_dict)
    
    def run_query_dicts(self, conn: connector=None, verbose: bool=True, preview_only: bool=True):
        for query_dict in self.query_dicts:
            self.run_query_dict(query_dict, conn=conn, verbose=verbose, preview_only=preview_only)
            
# Returns True if metadata_table exists under SEGMENT.IDENTIFIES_METADATA
def metadata_table_exists(metadata_table: str, conn: connector=None, verbose:bool=True):
    existing_metadata_tables = find_existing_metadata_tables(conn=conn, verbose=verbose)
    return True if metadata_table in existing_metadata_tables else False
    
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

def create_and_run_metadata_tables(conn: connector=None, verbose: bool=True, preview_only: bool=True):
    [data_file,latest_df] = get_segment_table_dicts_df(load_latest=True)
    for segment_table_dict in get_segment_table_dicts(latest_df):
        metadata_table_obj = MetadataTable(segment_table_dict)
        metadata_table_obj.clone_metadata_table(conn=conn, verbose=verbose, preview_only=preview_only)
        metadata_table_obj.add_query_dicts()
        metadata_table_obj.run_query_dicts(conn=conn, verbose=verbose, preview_only=preview_only)

################################################
# Tests
################################################

def tests():
    print("all tests passed in", os.path.basename(__file__))

def main():
    conn = create_connector()
    create_and_run_metadata_tables(conn=conn, verbose=True, preview_only=True)
    print("done")
    
if __name__ == "__main__":
    main()

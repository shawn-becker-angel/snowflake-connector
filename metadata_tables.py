from tkinter import E
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from query_generator import query_batch_generator, create_connector, execute_batched_select_query, execute_single_query, execute_count_query, execute_simple_query
from constants import *   
from typing import Set, List, Dict, Optional, Tuple

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
        
    

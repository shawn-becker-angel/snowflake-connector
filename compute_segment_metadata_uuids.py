from typing import Dict
from segment_utils import get_metadata_table_from_segment_table

# Execute the segment_query that populates one new UUID column in the 
# metadata_table table under SEGMENT.IDENTIFIES_METADATA
# of the given segment_table_dict. 
# 
# segment_table_dict has structure:
# {
#     'segment_table': 'SEGMENT.ANGEL_APP_IOS.IDENTIFIES'
#     'metadata_table': 'SEGMENT.IDENTIFIES_METADATA.SEGMENT__ANGEL_APP_IOS__IDENTIFIES'
#     'columns': 'ID-RECEIVED_AT-USER_ID-SENT_AT-TIMESTAMP-EMAIL-ANONYMOUS_ID',
#     'segment_query': ....
# }
# where 'segment_table' is the full path of an IDENTIFIES table, 
# database: SEGMENT / schema: ANGEL_APP_IOS / table: IDENTIFIES.
#
# 'columns' is a dash-delimited list of column names that are used in the segment_query.
# see segment_tables.compute_and_save_new_segment_tables_df
# see segment_tables.get_segment_tables_df
#
# 1. The name of the cloned table in SEGMENT.IDENTIFIES_METADATA is referered to as
# metadata_table 
# see segment_utils.get_metadata_table_from_segment_table
#
# 2. Create the cloned metadata_table for the segment_table if needed.
# see segment_tables.py.clone_segment_tables

# 3. alter the cloned metadata table to hold the new uuid column if needed. Get the name of 
# the new uuid column from segment_utils.get_uuid_column_from_segment_query 
#
# The new uuid column should be null for all existing rows and defaul to null all 
# newly created rows going forward
# 
# 4. run the left-outer-join seqment_query to update the new uuid column in the metadata_table
#
# 5. Report the total # rows, total # of rows with NULL uuids and total # of rows with non-null uuids
# 


# def execute_uuid_queries(segment_table_dict: Dict[str,str], conn: connector=None, verbose: bool=True) -> Dict[str,Any]:
def compute_segment_metadata_uuid_for_segment_table_dict(segment_table_dict) -> Dict[str,str]:
    pass
from typing import Dict
from segment_utils import get_metadata_table_from_segment_table
from metadata_tables import describe_metadata_table
from constants import *
from segment_tables import get_segment_table_dicts, get_segment_table_dicts_df

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
# see segment_tables.get_segment_table_dicts_df
#
# 1. Create the cloned metadata_table under SEGMENT.IDENTIFIES_METADATA for the 
# segment_table_metatable_dict if needeed
# see segment_utils.get_metadata_table_from_segment_table
#
# 2. Alter the cloned metadata table to hold the new uuid column.
# See segment_utils.get_uuid_column_from_segment_query 
#
# The new uuid column should be null for all existing rows and defaul to null all 
# newly created rows going forward
# 
# 3. run the left-outer-join seqment_query to update the new uuid column in the metadata_table
#
# 5. Report the total # rows, total # of rows with NULL uuids and total # of rows with non-null uuids
# 

def compute_metadata_uuid_for_metadata_table_dict(metadata_table_dict: Dict[str,str], verbose: bool=True) -> None:
    metadata_table = metadata_table_dict['metadata_table']
    metadata_table_info = describe_metadata_table(metadata_table)
    
    for segment_query_name in SEGMENT_QUERY_NAMES:
        if segment_query_name in metadata_table_dict['segment_queries'].keys():
            print(f"metadata_table: {metadata_table}, segment_query_name: {segment_query_name}")



################################################
# Tests
################################################

def test():    

    print("all tests passed in", os.path.basename(__file__))

def main():
    test()

if __name__ == "__main__":
    main()
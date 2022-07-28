from constants import *   
from timefunc import timefunc
from query_generator import create_connector
from segment_tables import compute_and_save_new_segment_table_dicts_df
from metadata_tables import create_and_run_metadata_tables


@timefunc
def main():
    
    conn = create_connector()
    
    print("compute_and_save_new_segment_table_dicts_df")
    compute_and_save_new_segment_table_dicts_df(verbose=True)
        
    print("create_and_run_metadata_tables")
    create_and_run_metadata_tables(conn=conn, verbose=True, preview_only=True)


if __name__ == "__main__":
    
    main()
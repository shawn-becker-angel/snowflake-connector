import sys
import os
import pandas as pd
import snowflake.connector as connector
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import Set, List, Dict, Optional, Tuple
from query_generator import create_connector
from segment_tables import compute_and_save_new_segment_tables_df
from batch_augmented_users import compute_and_save_metadata_for_all_segment_tables


@timefunc
def main():
    
    conn = create_connector()
    
    print("compute_and_save_new_segment_tables_df")
    (csv_file, segment_tables_df) = compute_and_save_new_segment_tables_df(verbose=True, conn=conn)
    
    segment_tables = list(segment_tables_df['segment_table'])
    print("num segment_tables:", len(segment_tables_df), "in:", csv_file)
        
    print("compute_and_save_metadata_for_all_segment_tables")
    compute_and_save_metadata_for_all_segment_tables(verbose=True, conn=conn)

if __name__ == "__main__":
    
    main()
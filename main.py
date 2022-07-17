import sys
import os
import glob
import math
import pandas as pd
import snowflake.connector as connector
from snowflake.connector import ProgrammingError
from constants import *   
from timefunc import timefunc
from time import sleep, perf_counter
from typing import Set, List, Dict, Optional, Tuple
import datetime
from functools import cache
from query_generator import create_connector, query_batch_generator
from utils import is_empty_list
from augmented_segment_users import compute_augmented_segment_users_for_all_segment_tables
from segment_tables import  get_segment_tables_df
from key_column_infos import get_key_column_infos


@timefunc
def main():
    
    conn = create_connector()
    
    # get (load or compute) the list of all queryable segment_tables with all key columns
    print("get_segment_tables_df")
    (csv_file, segment_tables_df) = get_segment_tables_df(verbose=False, conn=conn)
    segment_tables = list(segment_tables_df['segment_table'])
    print("num segment_tables:", len(segment_tables_df), "in:", csv_file)
    
    # # get (load or compute) the DataFrame of key_column_infos over a fixed period of time for the given list of segment_tables
    # print("get_key_column_infos")
    # (csv_file, key_column_infos_df) = get_key_column_infos(segment_tables)
    # print("num key_column_infos:", len(key_column_infos_df), "in:", csv_file)
    
    # get the list of segmented users for each segment_table
    # that have been augmented with with matching ellis_island_user uuids
    print("get_augmented_segment_users")
    compute_augmented_segment_users_for_all_segment_tables(conn=conn)

if __name__ == "__main__":
    
    main()
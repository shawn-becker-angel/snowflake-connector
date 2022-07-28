from constants import *   
from timefunc import timefunc
from query_generator import create_connector
from segment_tables import compute_and_save_new_segment_tables_df

@timefunc
def main():
    
    conn = create_connector()
    
    print("compute_and_save_new_segment_tables_df")
    (csv_file, segment_tables_df) = compute_and_save_new_segment_tables_df(verbose=True)
    
    segment_tables = list(segment_tables_df['segment_table'])
    print("num segment_tables:", len(segment_tables_df), "in:", csv_file)

if __name__ == "__main__":
    
    main()
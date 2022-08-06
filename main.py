from constants import *   
from timefunc import timefunc
from query_generator import create_connector
from segment_tables import compute_and_save_new_segment_table_dicts_df
from metadata_tables import create_and_run_metadata_tables, summarize_metadata_tables
from data_frame_utils import save_data_frame, load_data_frame


@timefunc
def main():
    
    conn = create_connector()
    
    print("compute_and_save_new_segment_table_dicts_df")
    compute_and_save_new_segment_table_dicts_df(verbose=True)
        
    print("create_and_run_metadata_tables")
    union_df = create_and_run_metadata_tables(conn=conn, verbose=True, preview_only=True)

    if union_df is not None:
        base_name = "union_df"
        saved_csv_file = save_data_frame(base_name, union_df, CSV_FORMAT)
        print(f"union_df saved to {saved_csv_file}")
        print(union_df)

        loaded_df = load_data_frame(saved_csv_file)
        print(f"loaded_df loaded from {saved_csv_file}:\n", loaded_df)
        print(loaded_df)
        
    summarize_metadata_tables(conn=conn, verbose=True)

if __name__ == "__main__":
    main()
    print("done")
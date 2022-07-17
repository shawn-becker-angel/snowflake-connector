import os

# Returns info_schema_table_name: SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
# given segment_table: SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
def get_info_schema_table_name_from_segment_table(segment_table):
    # handle special case #111
    segment_table = segment_table.replace("LOOKER_SOURCE.PUBLIC.","")
    return segment_table.replace(".","__")

# Returns segment_table: SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN
# given info_schema_table_name: SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN
def get_segment_table_from_info_schema_table_name(info_schema_table_name):
    segment_table = info_schema_table_name.replace("__",".")
    parts = segment_table.split(".")

    # special case #111: 
    # set database and schema prefix for segment_table when only table part is available.
    # e.g. like 'ANGL_APP_OPN_TO_PIF'
    if len(parts) == 1:
        segment_table = 'LOOKER_SOURCE.PUBLIC.' + segment_table
        
    return segment_table

################################################
# Tests
################################################

def test_conversions():
    schema_table_name = "SEGMENT__THE_CHOSEN_APP_WEB_PROD__LIVESTREAM_VIEW_STORE_BTN"
    segment_table_name = "SEGMENT.THE_CHOSEN_APP_WEB_PROD.LIVESTREAM_VIEW_STORE_BTN"
    
    input = schema_table_name
    expected = segment_table_name
    result = get_segment_table_from_info_schema_table_name(input)
    assert result == expected, f"ERROR: got {result} not expected {segment_table_name}"

    input = segment_table_name
    expected = schema_table_name
    result = get_info_schema_table_name_from_segment_table(input)
    assert result == expected, f"ERROR: got {result} not expected {segment_table_name}"
    
    input = "ANGL_APP_OPN_TO_PIF"
    expected = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF'
    result = get_segment_table_from_info_schema_table_name(input)
    assert result == expected, f"ERROR: got {result} not {expected}"

    input = 'LOOKER_SOURCE.PUBLIC.ANGL_APP_OPN_TO_PIF'
    expected = "ANGL_APP_OPN_TO_PIF"
    result = get_info_schema_table_name_from_segment_table(input)
    assert result == expected, f"ERROR: got {result} not {expected}"
    
    print("all tests passed in", os.path.basename(__file__))

def tests():
    test_conversions()

def main():
    tests()

if __name__ == "__main__":
    main()
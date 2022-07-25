import os
import glob
from typing import List, Optional
import pandas as pd
import string
import random
import pathlib


# Converts string v to a bool
def str2bool(v: str) -> bool:
    return v.lower() in ("yes", "true", "t", "1")

def is_empty_list(v: List) -> bool:
    return True if v is None or len(v) == 0 else False

# Returns True if name contains any filter in filters
def matches_any(name: str, filters: List[str]) -> bool:
    for filter in filters:
        if filter in name:
            return True
    return False

def is_readable_file(file_path: str) -> bool:
   return False if \
       (file_path is None) or \
           (len(file_path) == 0) or \
               (os.path.isfile(file_path) is False) or \
                   (os.access(file_path, os.R_OK) is False) \
        else True

# Returns the latest file that matches the given pattern
# or None if no such file found.
#
# For example: pattern="/tmp/ellis_island_users-*.csv"
# returns the most recent ellis_island_users csv file under /tmp
#
def find_latest_file(pattern) -> Optional[str]:
    # get list of files that matches pattern
    files = list(filter(os.path.isfile, glob.glob(pattern)))
    if files is not None and len(files) > 0:
        # sort by modified time
        files.sort(key=lambda x: os.path.getmtime(x))
        # return last item in list
        return files[-1]
    return None

# Returns exension ".txt" for file_name "my_file.txt" 
# from https://www.geeksforgeeks.org/how-to-get-file-extension-in-python/# 
def get_file_name_extension(file_name: str) -> str:
    return pathlib.Path(file_name).suffix

# from https://www.geeksforgeeks.org/python-generate-random-string-of-given-length/
def generate_random_string(N: int=7):
    res = ''.join(random.choices(
        string.ascii_uppercase + string.digits, k=N))
    return str(res)



################################################
# Tests
################################################
    
def test_str2bool():
    assert str2bool('') is False, "ERROR: empty string failure"
    assert str2bool('apple') is False, "ERROR: apple string failure"
    assert str2bool('0') is False, "ERROR: zero string failure"
    assert str2bool('1') is True, "ERROR: 1 string failure"
    assert str2bool('true') is True, "ERROR: true string failure"
    assert str2bool('TRUE') is True, "ERROR: TRUE string failure"
    assert str2bool('TrUe') is True, "ERROR: TrUe string failure"
    assert str2bool('y') is False, "ERROR: y string failure"
    assert str2bool('Y') is False, "ERROR: Y string failure"
    assert str2bool('yes') is True, "ERROR: yes string failure"
    assert str2bool('Yes') is True, "ERROR: Yes string failure"
    assert str2bool('t') is True, "ERROR: t string failure"
    assert str2bool('T') is True, "ERROR: T string failure"
    assert str2bool('f') is False, "ERROR: f string failure"
    assert str2bool('F') is False, "ERROR: F string failure"
    assert str2bool('false') is False, "ERROR: false string failure"
    assert str2bool('False') is False, "ERROR: False string failure"
    assert str2bool('FALSE') is False, "ERROR: FALSE string failure"
    try:
        assert str2bool(0) is False, "ERROR: int zero failure"
    except  AttributeError:
        pass
    try:
        assert str2bool(1) is False, "ERROR: int one failure"
    except  AttributeError:
        pass

def test_is_empty_list():
    assert is_empty_list([1,2,3]) is False, "ERROR: non-empty list failure"
    assert is_empty_list('apple') is False, "ERROR: string failure"
    assert is_empty_list('') is True, "ERROR: empty string failure"
    assert is_empty_list([]) is True, "ERROR: empty list failure"
    assert is_empty_list(None) is True, "ERROR: None list failure"
    try:
        assert is_empty_list(0) is False, "ERROR: zero failure"
    except TypeError:
        pass
    


def test_matches_any():
    filters = ['a', 'b']
    assert matches_any('a', filters) is True, "ERROR: matches_any True failure"
    assert matches_any('c', filters) is False, "ERROR: matches_any False failure"
    try:
        assert matches_any(0, filters) is False
    except TypeError:
        pass
    try:
        assert matches_any(None, filters) is False
    except TypeError:
        pass
    try:
        assert matches_any('a', None) is False
    except TypeError:
        pass

    try:
        assert matches_any(None, None) is False, "ERROR: matches_any None None failure"
    except TypeError:
        pass


def tests():
    test_str2bool()
    test_is_empty_list()
    test_matches_any()
    print("all tests passed in", os.path.basename(__file__))

        
def main():
    tests()

if __name__ == "__main__":
    main()
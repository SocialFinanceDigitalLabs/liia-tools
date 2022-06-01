"""
Set of dictionaries for use as configuration for la_agg.py for SSDA903 files
"""

# Column names for each 903 file.
# These are used for checking that the uploaded files match and identifying which CSV file is which (when CSVs are selected).
column_names = {
    'Header': ['CHILD', 'SEX', 'DOB', 'ETHNIC', 'UPN', 'MOTHER', 'MC_DOB', 'LA', 'YEAR'],
    'Episodes': ['CHILD', 'DECOM', 'RNE', 'LS', 'CIN', 'PLACE', 'PLACE_PROVIDER', 'DEC', 'REC', 'REASON_PLACE_CHANGE',
                 'HOME_POST', 'PL_POST', 'URN', 'LA', 'YEAR'],
    'Reviews': ['CHILD', 'DOB', 'REVIEW', 'REVIEW_CODE', 'LA', 'YEAR'],
    'UASC': ['CHILD', 'SEX', 'DOB', 'DUC', 'LA', 'YEAR'],
    'OC2': ['CHILD', 'DOB', 'SDQ_SCORE', 'SDQ_REASON', 'CONVICTED', 'HEALTH_CHECK',
            'IMMUNISATIONS', 'TEETH_CHECK', 'HEALTH_ASSESSMENT', 'SUBSTANCE_MISUSE',
            'INTERVENTION_RECEIVED', 'INTERVENTION_OFFERED', 'LA', 'YEAR'],
    'OC3': ['CHILD', 'DOB', 'IN_TOUCH', 'ACTIV', 'ACCOM', 'LA', 'YEAR'],
    'AD1': ['CHILD', 'DOB', 'DATE_INT', 'DATE_MATCH', 'FOSTER_CARE', 'NB_ADOPTR', 'SEX_ADOPTR', 'LS_ADOPTR', 'LA', 'YEAR'],
    'PlacedAdoption': ['CHILD', 'DOB', 'DATE_PLACED', 'DATE_PLACED_CEASED', 'REASON_PLACED_CEASED', 'LA', 'YEAR'],
    'PrevPerm': ['CHILD', 'DOB', 'PREV_PERM', 'LA_PERM', 'DATE_PERM', 'LA', 'YEAR'],
    'Missing': ['CHILD', 'DOB', 'MISSING', 'MIS_START', 'MIS_END', 'LA', 'YEAR']
}

# Date fields in each file type
# Fields need to be re-parsed as dates in order for sort functions to be applied correctly
dates = {
    'Header': ['DOB', 'MC_DOB'],
    'Episodes': ['DECOM', 'DEC'],
    'Reviews': ['REVIEW'],
    'UASC': ['DOB', 'DUC'],
    'OC2': ['DOB'],
    'OC3': ['DOB'],
    'AD1': ['DOB', 'DATE_INT', 'DATE_MATCH'],
    'PlacedAdoption': ['DOB', 'DATE_PLACED', 'DATE_PLACED_CEASED'],
    'PrevPerm': ['DOB', 'DATE_PERM'],
    'Missing': ['DOB', 'MIS_START', 'MIS_END']
}

# Columns used to sort rows in aggregated files
# Some of these are placeholders for if LAs submit multiple files for the same year
sort_order = {
    'Header': ['MC_DOB'],
    'Episodes': ['DEC'],
    'UASC': ['DUC']
}

# Columns used to de-duplicate files when aggregated
dedup = {
    'Header': ['CHILD', 'YEAR'],
    'Episodes': ['CHILD', 'DECOM', 'RNE', 'LS', 'PLACE', 'PLACE_PROVIDER', 'PL_POST'],
    'Reviews': ['CHILD', 'REVIEW'],
    'UASC': ['CHILD'],
    'AD1': ['CHILD', 'DATE_INT', 'DATE_MATCH'],
    'PlacedAdoption': ['DATE_PLACED', 'DATE_PLACED_CEASED'],
    'PrevPerm': ['CHILD', 'DATE_PERM']
}
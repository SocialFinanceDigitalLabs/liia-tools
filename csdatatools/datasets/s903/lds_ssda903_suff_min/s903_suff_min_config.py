"""
Set of dictionaries for use as configuration for s903_pan_agg.py for SSDA903 files
"""

# Column names for each 903 file.
# These are used for checking that the uploaded files match and identifying which CSV file is which (when CSVs are selected).
# Note that by this point in the process, two of the original SSDA903 files (AD1 and PlacedAdoption) have already been removed as part of data minimisation
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
    'PrevPerm': ['CHILD', 'DOB', 'PREV_PERM', 'LA_PERM', 'DATE_PERM', 'LA', 'YEAR'],
    'Missing': ['CHILD', 'DOB', 'MISSING', 'MIS_START', 'MIS_END', 'LA', 'YEAR']
}

# Datasets to be retained for pan-London sufficiency analysis; other tables will not be passed through
suff_data_kept = ['Header', 'Episodes', 'UASC', 'OC2', 'Missing']

# Fields in each of the retained datasets that are removed for pan-London sufficiency analysis
minimise = {
    'Episodes': [],
    'Header': ['UPN', 'MOTHER', 'MC_DOB'],
    'UASC': [],
    'OC2': ['HEALTH_CHECK', 'IMMUNISATIONS', 'TEETH_CHECK', 'HEALTH_ASSESSMENT'],
    'Missing': []
}
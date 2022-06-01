from pathlib import Path
import re
import pandas as pd
from datetime import datetime
import numpy as np

from cc_pan_agg_config import ref_assessment
from cc_pan_agg_config import s47_cpp_days
from cc_pan_agg_config import icpc_cpp_days

def read_input_file(root_directory):
    '''Finds the filepath of the file that launches this process'''
# This will need to be replaced on the London DataStore with a suitable mechanism for
# identifying a newly processed file and passing that file through this process
    filepath = Path(root_directory, r"LA folders\Bromley\CIN Census\Outputs\CIN_Census_merged_flatfile.csv") # String needs to be replaced with way of identifying the file to be processed
    return filepath


def read_file(file):
    '''Reads the csv file as a pandas DataFrame'''
    df = pd.read_csv(file)
    return df


def find_la_name(filepath):
    match = re.search(r"LA folders\\(.*?)\\CIN Census", str(filepath))
    assert match is not None, f"No local authority found in {filepath}"
    la_name = match.group(1)
    return la_name


def find_pan_folder(root_directory):
    pan_folder = Path(root_directory, r"LIIA analysis folder\Structural Inequalities Benchmarking\CIN Census\Flatfile")
    return pan_folder


def merge_agg_files(pan_folder, la_name, new_df):
    file_list = Path(pan_folder).glob('*.csv')
    for file in file_list:
        old_df = pd.read_csv(file)
        old_df = old_df.drop(old_df[old_df['LA'] == la_name].index)
        new_df = pd.concat([new_df, old_df], axis=0)
    return new_df


def export_flatfile(pan_folder, df):
    output_path = Path(pan_folder, f"pan_London_CIN_Census_flatfile.csv")
    df.to_csv(output_path, index=False)


def assessments_inputs(df):
    '''Reduces rows to only assessment events
       Removes redundant columns that relate to other types of event'''
    df = df[df['Type'] == 'AssessmentAuthorisationDate']
    df = df.dropna(axis=1, how='all')
    return df


def split_assessments(df):
    '''Creates a new set of columns from the flatfile with a column for each assessment factor
       Rows correspond the the rows of the flatfile and should have a value of 0 or 1 for each column'''
    factor_cols = df.Factors.str.split(',', expand=True).stack().str.get_dummies().sum(level=0)
    assert factor_cols.isin([0,1]).all().all()
    df = pd.concat([df, factor_cols], axis=1)
    return df


def export_factfile(pan_folder, df):
    '''Writes the new assessment factors file as a csv in the LIIA Analysis folder'''
    output_path = str(pan_folder)
    output_path = re.sub("Flatfile", "Analysis", output_path)
    output_path = Path(output_path, f"pan_London_assessments.csv")
    df.to_csv(output_path, index=False)


def referral_inputs(df):
    '''Creates three views needed for generating referral journeys analysis file'''
    ref = df[df["Type"] == "CINreferralDate"]
    ref = ref.dropna(axis=1, how='all')
    s17 = df[df["Type"] == "AssessmentActualStartDate"]
    s17 = s17.dropna(axis=1, how='all')
    s47 = df[df["Type"] == "S47ActualStartDate"]
    s47 = s47.dropna(axis=1, how='all')
    return ref, s17, s47


def merge_ref_s17(ref, s17):
    '''Merges ref and s17 views together, keeping only logically valid matches'''
    data_s17 = ref.merge(s17[['LAchildID', 'AssessmentActualStartDate']], how='left', on='LAchildID')
    data_s17["days_to_s17"] = data_s17['AssessmentActualStartDate'] - data_s17['CINreferralDate']
    data_s17["days_to_s17"] = data_s17["days_to_s17"].dt.days
    # Only s17 events after (>0) but within 30 days (<= ref_assessment) of referral events are valid
    ref_s17 = data_s17[((data_s17["days_to_s17"] <= ref_assessment) & (data_s17["days_to_s17"] >=0))]
    ref_s17 = ref_s17[['Date', 'LAchildID', 'AssessmentActualStartDate', 'days_to_s17']]
    return ref_s17


def merge_ref_s47(ref, s47):
    '''Merges ref and s47 views together, keeping only logically valid matches'''
    data_s47 = ref.merge(s47[['LAchildID', 'S47ActualStartDate']], how='left', on='LAchildID')
    data_s47["days_to_s47"] = data_s47['S47ActualStartDate'] - data_s47['CINreferralDate']
    data_s47["days_to_s47"] = data_s47["days_to_s47"].dt.days
    # Only s47 events after (>0) but within 30 days (<= ref_assessment) of referral events are valid
    ref_s47 = data_s47[((data_s47["days_to_s47"] <= ref_assessment) & (data_s47["days_to_s47"] >=0))]
    ref_s47 = ref_s47[['Date', 'LAchildID', 'S47ActualStartDate', 'days_to_s47']]
    return ref_s47


def ref_outcomes(ref, ref_s17, ref_s47):
    '''Merges views together as outcomes of referrals
       Outcomes column defaults to NFA unless there is a relevant S17 or S47 event to match
       Calculates age of child at referral'''
    ref_outs = ref.merge(ref_s17, on=['Date', 'LAchildID'], how='left')
    ref_outs = ref_outs.merge(ref_s47, on=['Date', 'LAchildID'], how='left')
    # Set default outcome to "NFA"
    ref_outs["referral_outcome"] = "NFA"
    # Set outcome to "S17" when there is a relevant assessment
    ref_outs.loc[ref_outs["AssessmentActualStartDate"].notnull(), "referral_outcome"] = "S17"
    # Set outcome to "S47" when there is a relevant S47
    ref_outs.loc[ref_outs["S47ActualStartDate"].notnull(), "referral_outcome"] = "S47"
    # Set outcome to "Both S17 & S47" when there are both
    ref_outs.loc[((ref_outs["AssessmentActualStartDate"].notnull()) & (ref_outs["S47ActualStartDate"].notnull())), "referral_outcome"] = "Both S17 & S47"
    # Calculate age of child at referral
    ref_outs["Age at referral"] = ref_outs['CINreferralDate'] - ref_outs['PersonBirthDate']
    ref_outs["Age at referral"] = ref_outs["Age at referral"].dt.days
    ref_outs["Age at referral"] = (ref_outs["Age at referral"] / 365.25).apply(np.floor)
    ref_outs["Age at referral"] = ref_outs["Age at referral"].astype(int)
    return ref_outs


def export_reffile(pan_folder, df):
    '''Writes the new referral journeys file as a csv in the LIIA Analysis folder'''
    output_path = str(pan_folder)
    output_path = re.sub("Flatfile", "Analysis", output_path)
    output_path = Path(output_path, f"CIN_Census_referrals.csv")
    df.to_csv(output_path, index=False)


def journey_inputs(df):
    '''Creates the input view for the journey analysis file'''
    s47_j = df[df["Type"] == "S47ActualStartDate"]
    s47_j = s47_j.dropna(axis=1, how='all')
    cpp = df[df["Type"] == "CPPstartDate"]
    cpp = cpp.dropna(axis=1, how='all')
    s47_cpp = s47_j.merge(cpp[["LAchildID", "CPPstartDate"]], how='left', on="LAchildID")
    # Calculate days from ICPC to CPP start
    s47_cpp["icpc_to_cpp"] = s47_cpp["CPPstartDate"] - s47_cpp["DateOfInitialCPC"]
    s47_cpp["icpc_to_cpp"] = s47_cpp["icpc_to_cpp"].dt.days
    # Calculate days from S47 to CPP start
    s47_cpp["s47_to_cpp"] = s47_cpp["CPPstartDate"] - s47_cpp["S47ActualStartDate"]
    s47_cpp["s47_to_cpp"] = s47_cpp["s47_to_cpp"].dt.days
    # Only keep logically consistent events (as defined in config variables)
    s47_cpp = s47_cpp[((s47_cpp["icpc_to_cpp"] >= 0) & (s47_cpp["icpc_to_cpp"] <= icpc_cpp_days)) | 
                     ((s47_cpp["s47_to_cpp"] >= 0) & (s47_cpp["s47_to_cpp"] <= s47_cpp_days))]
    # Merge events back to S47_j view
    s47_outs = s47_j.merge(s47_cpp[['Date', 'LAchildID', 'CPPstartDate', 'icpc_to_cpp', 's47_to_cpp']], how='left', on=['Date', 'LAchildID'])
    return s47_outs


def s47_paths(df):
    '''Creates an output that can generate a Sankey diagram of outcomes from S47 events'''
    # Dates used to define window for S47 events where outcome may not be known because CIN Census is too recent
    df["cin_census_close"] = datetime(df["YEAR"], 3, 31)
    df["s47_max_date"] = df["cin_census_close"] - pd.Timedelta("60 days")
    df["icpc_max_date"] = df["cin_census_close"] - pd.Timedelta("45 days")
    # Setting the Sankey diagram source for S47 events
    step1 = df.copy()
    step1["Source"] = 'S47 strategy discussion'
    # Setting the Sankey diagram destination for S47 events
    step1["Destination"] = np.nan
    step1.loc[step1["DateOfInitialCPC"].notnull(), "Destination"] = 'ICPC'
    step1.loc[step1["DateOfInitialCPC"].isnull() & step1["CPPstartDate"].notnull(), "Destination"] = 'CPP start'
    step1.loc[((step1["Destination"].isnull()) & (step1["S47ActualStartDate"] >= step1["s47_max_date"])), "Destination"] = 'TBD - S47 too recent'
    step1.loc[step1["Destination"].isnull(), "Destination"] = 'No ICPC or CPP'
    # Setting the Sankey diagram source for ICPC events
    step2 = step1[step1["Destination"] == 'ICPC']
    step2["Source"] = 'ICPC'
    # Setting the Sankey diagram destination for ICPC events
    step2["Destination"] = np.nan
    step2.loc[step2["CPPstartDate"].notnull(), "Destination"] = 'CPP start'
    step2.loc[((step2["Destination"].isnull()) & (step2["DateOfInitialCPC"] >= step2["icpc_max_date"])), "Destination"] = 'TBD - ICPC too recent'
    step2.loc[step2["Destination"].isnull(), "Destination"] = 'No CPP'
    # Merge the steps together
    s47_journey = pd.concat([step1, step2])
    # Calculate age of child at S47
    s47_journey["Age at S47"] = s47_journey['S47ActualStartDate'] - s47_journey['PersonBirthDate']
    s47_journey["Age at S47"] = s47_journey["Age at S47"].dt.days
    s47_journey["Age at S47"] = (s47_journey["Age at S47"] / 365.25).apply(np.floor)
    s47_journey["Age at S47"] = s47_journey["Age at S47"].astype(int)
    return s47_journey


def export_journeyfile(pan_folder, df):
    '''Writes the new S47 journeys file as a csv in the LA's Analysis folder'''
    output_path = str(pan_folder)
    output_path = re.sub("Flatfile", "Analysis", output_path)
    output_path = Path(output_path, f"CIN_Census_S47_journey.csv")
    df.to_csv(output_path, index=False)


def cc_pan_agg(root_directory):
    filepath = read_input_file(root_directory) # This function will need to be re-written to specify the file that has launched the process
    stream = read_file(filepath)
    la_name = find_la_name(filepath)
    pan_folder = find_pan_folder(root_directory)
    stream = merge_agg_files(pan_folder, la_name, stream)
    export_flatfile(pan_folder, stream)
    assessments = assessments_inputs(stream)
    assessments = split_assessments(assessments)
    export_factfile(pan_folder, assessments)
    ref, s17, s47 = referral_inputs(stream)
    ref_s17 = merge_ref_s17(ref, s17)
    ref_s47 = merge_ref_s47(ref, s47)
    ref_outs = ref_outcomes(ref, ref_s17, ref_s47)
    export_reffile(pan_folder, ref_outs)
    s47_outs = journey_inputs(stream)
    s47_journey = s47_paths(s47_outs)
    export_journeyfile(pan_folder, s47_journey)

LDS_root = Path(r"C:\Users\Michael.Hanks\OneDrive - Social Finance Ltd\Desktop\Fake LDS 2") # Literal directory reference to top folder of LDS LIIA directory
cc_pan_agg(LDS_root)
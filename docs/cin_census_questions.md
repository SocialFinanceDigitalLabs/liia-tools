# Questions about de-duplicating CIN Census

Whilst documenting these processes I came accross a lot of functions that seemed to be doing the same thing. And in porting the code to Dagster, any amount of deduplication
we can do will signficantly reduce the amount of work.

We also have the race condition issue, and if we can simplify parts of the process we can further minimise that risk. 

The current CIN process (as well as the two other 'standard' ones) are all based on the same pattern:

1. cleandata
2. la_agg
3. pan_agg

**Step 1** loads the input file (XML) and ensures that it is clean and outputs as a single CSV file in broad format. 

**Step 2** loads this CSV file and creates a number of views of this:

* 'CIN_Census_merged_flatfile' - this is 
* 'CIN_Census_factors.csv' - a child-level one-high view of the risk factors found in the data
* 'CIN_Census_referrals' - a child-level view of the referrals found in the data
* 'CIN_Census_S47_journey' - a child-level view of the journeys

The merged flatfile involves loading the merged flatfile from any previous run and merging it with the current input file, then deduplicating it.
    * I'm inferring from this that the data provided in each run is not 'complete' and so we need to merge it with the previous run to get a complete picture.
    * However, we overwrite this file on each run - so a corrupt run will remove any history and there is no backup facility. This could lead to data loss.

Note also there is a fair amount of duplication in this data as these are all views of the same data and so include all the child-level data for each child instead of 
having a child file and then much smaller views. 

Most analysis tools are able to do a join in child_id so this small change could potentially make a big difference to performance of any dashboards and later analysis, as
well as avoiding potentially conflicting data.

**Step 3** loads the data from all las and merges the data together. This is then used to create a number of views:

* 'pan_London_CIN_flatfile' - this is 
* 'CIN_Census_factors.csv' - a child-level one-high view of the risk factors found in the data
* 'CIN_Census_referrals' - a child-level view of the referrals found in the data
* 'CIN_Census_S47_journey' - a child-level view of the journeys

The pan_London_CIN_flatfile is created from the current LA file as well as the previous version of the pan_London_CIN_flatfile. Based on the LA Code (provided by CLI) all
columns for that LA are dropped and the new file merged in. 
    * The fact that the LA code is provided by the CLI leads me to suspect that, although not specified, the input file is the cleaned file for the LA, not the merged 
      flatfile as this has the la code in it. **CORRECTION:** The is intended to run from the merged flatfile. The LA code is used to drop the LA data from the pan-London file.
    * This means that the pan london flatfile will only have the latest data - not the historic data. It's not specified if this is intentional or accidental. 
      The code will happily accept either the merged flatfile or the cleaned file as input, so it's not clear which is the correct one.

Using the now merged view, this step proceeds to calculate all the child-level views as before.

My impression is that these "aggregate" files really are "concatenated" files as the data is still child-level, and that rather than running all the code again an approach that is simpler and less like to suffer from race conditions would be to:

* Clean the incoming LA file
* Using the clean filed - merged any history from timestamped 'state' files within the LA
* Create the child-level views for this LA

Any change to the child-level views for the LA would trigger the 'pan london' task to run, which would then:

* Load and concatenate all the child-level views for all the LAs - no calculation is required as the data is already in the right format
* Apply any privacy rules to the data that exist on the pan-london scale

This is computationally (and therefore code-maintenance wise) much simpler and less likely to suffer from any overwriting of files as each part of the code writes to a unique file.
There is still a slight risk in the 'state' files - so if it's essential that we don't drop data from earlier runs, we probably need a locking mechanism there.

Similarly the pan london files could be triggered by two different LAs at the same time, and thus produce slightly different outputs. Again, we could probably ensure only one merge task at the time can run, and the issue is less severe as the problem would "recover" on the next run as none of the source data is overwritten.

Finally it would remove large amounts of duplicate code thus making the code much simpler to maintain and making the migration to Dagster much simpler. See below.

## Examples

Here are some examples based on the sample data file provided

**&lt;inputfile>_clean.csv**

```csv
LAchildID,Date,Type,CINreferralDate,ReferralSource,PrimaryNeedCode,CINclosureDate,ReasonForClosure,DateOfInitialCPC,ReferralNFA,CINPlanStartDate,CINPlanEndDate,S47ActualStartDate,InitialCPCtarget,ICPCnotRequired,AssessmentActualStartDate,AssessmentInternalReviewDate,AssessmentAuthorisationDate,Factors,CPPstartDate,CPPendDate,InitialCategoryOfAbuse,LatestCategoryOfAbuse,NumberOfPreviousCPP,UPN,FormerUPN,UPNunknown,PersonBirthDate,ExpectedPersonBirthDate,GenderCurrent,PersonDeathDate,PersonSchoolYear,Ethnicity,Disabilities,YEAR,LA
DfEX0000001_BAD,1970-10-06,CINreferralDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-02-27,CINclosureDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1970-06-03,AssessmentActualStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,1970-06-03,1970-06-22,1971-07-18,"2A,2B",,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-07-18,AssessmentAuthorisationDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,1970-06-03,1970-06-22,1971-07-18,"2A,2B",,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-01-24,CINPlanStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,1971-01-24,1971-01-26,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-01-26,CINPlanEndDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,1971-01-24,1971-01-26,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1970-06-02,S47ActualStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-06-17,0,,,1970-06-02,1970-06-23,0,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1970-02-17,CPPstartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,1970-02-17,1971-03-14,PHY,PHY,10,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-03-14,CPPendDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,1970-02-17,1971-03-14,PHY,PHY,10,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-02-15,CPPreviewDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,1970-02-17,1971-03-14,PHY,PHY,10,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
```

**CIN_Census_merged_flatfile.csv**

```csv
LAchildID,Date,Type,CINreferralDate,ReferralSource,PrimaryNeedCode,CINclosureDate,ReasonForClosure,DateOfInitialCPC,ReferralNFA,CINPlanStartDate,CINPlanEndDate,S47ActualStartDate,InitialCPCtarget,ICPCnotRequired,AssessmentActualStartDate,AssessmentInternalReviewDate,AssessmentAuthorisationDate,Factors,CPPstartDate,CPPendDate,InitialCategoryOfAbuse,LatestCategoryOfAbuse,NumberOfPreviousCPP,UPN,FormerUPN,UPNunknown,PersonBirthDate,ExpectedPersonBirthDate,GenderCurrent,PersonDeathDate,PersonSchoolYear,Ethnicity,Disabilities,YEAR,LA
DfEX0000001_BAD,1970-06-02,S47ActualStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-06-17,0,,,1970-06-02,1970-06-23,0.0,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1970-06-03,AssessmentActualStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,1970-06-03,1970-06-22,1971-07-18,"2A,2B",,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-07-18,AssessmentAuthorisationDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,1970-06-03,1970-06-22,1971-07-18,"2A,2B",,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1970-10-06,CINreferralDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-02-27,CINclosureDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-01-24,CINPlanStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,1971-01-24,1971-01-26,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-01-26,CINPlanEndDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,1971-01-24,1971-01-26,,,,,,,,,,,,,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1970-02-17,CPPstartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,1970-02-17,1971-03-14,PHY,PHY,10.0,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-03-14,CPPendDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,1970-02-17,1971-03-14,PHY,PHY,10.0,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
DfEX0000001_BAD,1971-02-15,CPPreviewDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,,,,,,,,,,1970-02-17,1971-03-14,PHY,PHY,10.0,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham
```

**CIN_Census_factors.csv**

```csv 
LAchildID,Date,Type,CINreferralDate,ReferralSource,PrimaryNeedCode,CINclosureDate,ReasonForClosure,DateOfInitialCPC,ReferralNFA,AssessmentActualStartDate,AssessmentInternalReviewDate,AssessmentAuthorisationDate,Factors,UPN,FormerUPN,UPNunknown,PersonBirthDate,ExpectedPersonBirthDate,GenderCurrent,PersonDeathDate,PersonSchoolYear,Ethnicity,Disabilities,YEAR,LA,2A,2B
DfEX0000001_BAD,1971-07-18,AssessmentAuthorisationDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,1970-06-03,1970-06-22,1971-07-18,"2A,2B",A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham,1,1
```

**CIN_Census_referrals.csv**

```csv
LAchildID,Date,Type,CINreferralDate,ReferralSource,PrimaryNeedCode,CINclosureDate,ReasonForClosure,DateOfInitialCPC,ReferralNFA,UPN,FormerUPN,UPNunknown,PersonBirthDate,ExpectedPersonBirthDate,GenderCurrent,PersonDeathDate,PersonSchoolYear,Ethnicity,Disabilities,YEAR,LA,AssessmentActualStartDate,days_to_s17,S47ActualStartDate,days_to_s47,referral_outcome,Age at referral
DfEX0000001_BAD,1970-10-06,CINreferralDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-12-06,0,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham,,,,,NFA,4
```

**CIN_Census_S47_journey.csv**

```csv
LAchildID,Date,Type,CINreferralDate,ReferralSource,PrimaryNeedCode,CINclosureDate,ReasonForClosure,DateOfInitialCPC,ReferralNFA,S47ActualStartDate,InitialCPCtarget,ICPCnotRequired,UPN,FormerUPN,UPNunknown,PersonBirthDate,ExpectedPersonBirthDate,GenderCurrent,PersonDeathDate,PersonSchoolYear,Ethnicity,Disabilities,YEAR,LA,CPPstartDate,icpc_to_cpp,s47_to_cpp,cin_census_close,s47_max_date,icpc_max_date,Source,Destination,Age at S47
DfEX0000001_BAD,1970-06-02,S47ActualStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-06-17,0,1970-06-02,1970-06-23,0.0,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham,,,,2022-03-31,2022-01-30,2022-02-14,S47 strategy discussion,ICPC,4
DfEX0000001_BAD,1970-06-02,S47ActualStartDate,1970-10-06,1A,N4,1971-02-27,RC1,1970-06-17,0,1970-06-02,1970-06-23,0.0,A123456789123,X98765432123B,UN3,1966-03-01,1966-03-01,1,1980-10-01,1965,WBRI,"HAND,HEAR",2022,Barking and Dagenham,,,,2022-03-31,2022-01-30,2022-02-14,ICPC,No CPP,4
```

## Code Comparison

### CLI commands

Side by side comparison for the LA and PAN views. 

Beyond the initial loading steps, the code is identially apart from the package from which the functions are loaded. 

```sdiff
@cin_census.command()                                           @cin_census.command()
@click.option(                                                  @click.option(
    "--i",                                                          "--i",
    "input",                                                        "input",
    required=True,                                                  required=True,
    type=str,                                                       type=str,
    help="A string specifying the input file location, includ       help="A string specifying the input file location, includ
)                                                               )
@click.option(                                                  @click.option(
                                                              >     "--la_code",
                                                              >     required=True,
                                                              >     type=click.Choice(la_list, case_sensitive=False),
                                                              >     help="A three letter code, specifying the local authority
                                                              > )
                                                              > @click.option(
    "--flat_output",                                                "--flat_output",
    required=True,                                                  required=True,
    type=str,                                                       type=str,
    help="A string specifying the directory location for the        help="A string specifying the directory location for the 
)                                                               )
@click.option(                                                  @click.option(
    "--analysis_output",                                            "--analysis_output",
    required=True,                                                  required=True,
    type=str,                                                       type=str,
    help="A string specifying the directory location for the        help="A string specifying the directory location for the 
)                                                               )
def la_agg(input, flat_output, analysis_output):              | def pan_agg(input, la_code, flat_output, analysis_output):
    """                                                             """
    Joins data from newly cleaned CIN Census file (output of  |     Joins data from newly merged CIN Census file (output of l
    :param input: should specify the input file location, inc       :param input: should specify the input file location, inc
                                                              >     :param la_code: should be a three-letter string for the l
    :param flat_output: should specify the path to the folder       :param flat_output: should specify the path to the folder
    :param analysis_output: should specify the path to the fo       :param analysis_output: should specify the path to the fo
    :return: None                                                   :return: None
    """                                                             """
                                                                
    # Configuration                                                 # Configuration
    config = agg_config.Config()                              |     config = pan_config.Config()
                                                                
    # Open file as Dataframe                                  |     # Create flat file
    dates = config["dates"]                                         dates = config["dates"]
    flatfile = agg_process.read_file(input, dates)            |     flatfile = pan_process.read_file(input, dates)
                                                                
    # Merge with existing data, de-duplicate and apply data r |     # Merge with existing pan-London data
    flatfile = agg_process.merge_la_files(flat_output, dates, |     la_name = flip_dict(config["data_codes"])[la_code]
    sort_order = config["sort_order"]                         |     flatfile = pan_process.merge_agg_files(flat_output, dates
    dedup = config["dedup"]                                   <
    flatfile = agg_process.deduplicate(flatfile, sort_order,  <
    flatfile = agg_process.remove_old_data(flatfile, years=6) <
                                                                
    # Output flatfile                                               # Output flatfile
    agg_process.export_flatfile(flat_output, flatfile)        |     pan_process.export_flatfile(flat_output, flatfile)
                                                                
    # Create and output factors file                                # Create and output factors file
    factors = agg_process.filter_flatfile(                    |     factors = pan_process.filter_flatfile(
        flatfile, filter="AssessmentAuthorisationDate"                  flatfile, filter="AssessmentAuthorisationDate"
    )                                                               )
    if len(factors) > 0:                                            if len(factors) > 0:
        factors = agg_process.split_factors(factors)          |         factors = pan_process.split_factors(factors)
        agg_process.export_factfile(analysis_output, factors) |         pan_process.export_factfile(analysis_output, factors)
                                                                
    # Create referral file                                          # Create referral file
    ref, s17, s47 = agg_process.referral_inputs(flatfile)     |     ref, s17, s47 = pan_process.referral_inputs(flatfile)
    if len(s17) > 0 and len(s47) > 0:                               if len(s17) > 0 and len(s47) > 0:
        ref_assessment = config["ref_assessment"]                       ref_assessment = config["ref_assessment"]
        ref_s17 = agg_process.merge_ref_s17(ref, s17, ref_ass |         ref_s17 = pan_process.merge_ref_s17(ref, s17, ref_ass
        ref_s47 = agg_process.merge_ref_s47(ref, s47, ref_ass |         ref_s47 = pan_process.merge_ref_s47(ref, s47, ref_ass
        ref_outs = agg_process.ref_outcomes(ref, ref_s17, ref |         ref_outs = pan_process.ref_outcomes(ref, ref_s17, ref
        agg_process.export_reffile(analysis_output, ref_outs) |         pan_process.export_reffile(analysis_output, ref_outs)
                                                                
    # Create journey file                                           # Create journey file
    icpc_cpp_days = config["icpc_cpp_days"]                         icpc_cpp_days = config["icpc_cpp_days"]
    s47_cpp_days = config["s47_cpp_days"]                           s47_cpp_days = config["s47_cpp_days"]
    s47_j, cpp = agg_process.journey_inputs(flatfile)         |     s47_j, cpp = pan_process.journey_inputs(flatfile)
    if len(s47_j) > 0 and len(cpp) > 0:                             if len(s47_j) > 0 and len(cpp) > 0:
        s47_outs = agg_process.journey_merge(s47_j, cpp, icpc |         s47_outs = pan_process.journey_merge(s47_j, cpp, icpc
        s47_day_limit = config["s47_day_limit"]                         s47_day_limit = config["s47_day_limit"]
        icpc_day_limit = config["icpc_day_limit"]                       icpc_day_limit = config["icpc_day_limit"]
        s47_journey = agg_process.s47_paths(s47_outs, s47_day |         s47_journey = pan_process.s47_paths(s47_outs, s47_day
        agg_process.export_journeyfile(analysis_output, s47_j |         pan_process.export_journeyfile(analysis_output, s47_j
```

### Process Comparison

We can also look at the initial process files. Again we see that these files are identical beyond the initial loading of the files and a few comments that I have added.

This violates the principles of DRY as well as making the code much harder to understand and maintain. 

```sdiff
                                                              > import logging
from pathlib import Path                                        from pathlib import Path
import pandas as pd                                             import pandas as pd
from datetime import datetime                                   from datetime import datetime
import numpy as np                                              import numpy as np
import logging                                                <
                                                                
log = logging.getLogger(__name__)                               log = logging.getLogger(__name__)
                                                                
                                                                
def read_file(input, dates):                                    def read_file(input, dates):
    """                                                             """
    Reads the csv file as a pandas DataFrame                        Reads the csv file as a pandas DataFrame
    """                                                             """
    flatfile = pd.read_csv(input, parse_dates=dates, dayfirst       flatfile = pd.read_csv(input, parse_dates=dates, dayfirst
    return flatfile                                                 return flatfile
                                                                
                                                                
def merge_la_files(flat_output, dates, flatfile):             | def _merge_dfs(flatfile, old_df, la_name):
    """                                                             """
    Looks for existing file of the same type and merges with  |     Deletes existing data for new LA from pan file
                                                              >     Merges new LA data to pan file
    """                                                             """
    old_file = Path(flat_output, f"CIN_Census_merged_flatfile |     old_df = old_df.drop(old_df[old_df["LA"] == la_name].inde
    if old_file.is_file():                                    |     flatfile = pd.concat([flatfile, old_df], axis=0, ignore_i
        old_df = pd.read_csv(old_file, parse_dates=dates, day <
        merged_df = pd.concat([flatfile, old_df], axis=0)     <
    else:                                                     <
        merged_df = flatfile                                  <
    return merged_df                                          <
                                                              <
                                                              <
def deduplicate(flatfile, sort_order, dedup):                 <
    """                                                       <
    Sorts and removes duplicate records from merged files fol <
    """                                                       <
    flatfile = flatfile.sort_values(sort_order, ascending=Fal <
    flatfile = flatfile.drop_duplicates(subset=dedup, keep="f <
    return flatfile                                                 return flatfile
                                                                
                                                                
def remove_old_data(flatfile, years):                         | def merge_agg_files(flat_output, dates, la_name, flatfile):
    """                                                             """
    Removes data older than a specified number of years       |     Checks if pan file exists
                                                              >     Passes old and new file to function to be merged
    """                                                             """
    year = pd.to_datetime("today").year                       |     output_file = Path(flat_output, f"pan_London_CIN_flatfile
    month = pd.to_datetime("today").month                     |     if output_file.is_file():
    if month <= 6:                                            |         old_df = pd.read_csv(output_file, parse_dates=dates, 
        year = year - 1                                       |         flatfile = _merge_dfs(flatfile, old_df, la_name)
    flatfile = flatfile[flatfile["YEAR"] >= year - years]     <
    return flatfile                                                 return flatfile
                                                                
                                                                
def export_flatfile(flat_output, flatfile):                     def export_flatfile(flat_output, flatfile):
    """                                                       |     output_path = Path(flat_output, f"pan_London_CIN_flatfile
    Writes the flatfile output as a csv                       <
    """                                                       <
    output_path = Path(flat_output, f"CIN_Census_merged_flatf <
    flatfile.to_csv(output_path, index=False)                       flatfile.to_csv(output_path, index=False)
                                                                
                                                                
def filter_flatfile(flatfile, filter):                          def filter_flatfile(flatfile, filter):
    """                                                             """
    Filters rows to specified events                                Filters rows to specified events
    Removes redundant columns that relate to other types of e       Removes redundant columns that relate to other types of e
    """                                                             """
    filtered_flatfile = flatfile[flatfile["Type"] == filter]        filtered_flatfile = flatfile[flatfile["Type"] == filter]
    filtered_flatfile = filtered_flatfile.dropna(axis=1, how=       filtered_flatfile = filtered_flatfile.dropna(axis=1, how=
    return filtered_flatfile                                        return filtered_flatfile
                                                                
                                                                
def split_factors(factors):                                     def split_factors(factors):
    """                                                             """
    Creates a new set of columns from the flatfile with a col       Creates a new set of columns from the flatfile with a col
    Rows correspond the the rows of the flatfile and should h       Rows correspond the the rows of the flatfile and should h
    """                                                             """
    factor_cols = factors.Factors                                   factor_cols = factors.Factors
    factor_cols = factor_cols.str.split(",", expand=True)           factor_cols = factor_cols.str.split(",", expand=True)
    factor_cols = factor_cols.stack()                               factor_cols = factor_cols.stack()
    factor_cols = factor_cols.str.get_dummies()                     factor_cols = factor_cols.str.get_dummies()
    factor_cols = factor_cols.groupby(level=0).sum()                factor_cols = factor_cols.groupby(level=0).sum()
    assert factor_cols.isin([0, 1]).all(axis=None)                  assert factor_cols.isin([0, 1]).all(axis=None)
    factors = pd.concat([factors, factor_cols], axis=1)             factors = pd.concat([factors, factor_cols], axis=1)
    return factors                                                  return factors
                                                                
                                                                
def export_factfile(analysis_output, factors):                  def export_factfile(analysis_output, factors):
    """                                                             """
    Writes the factors output as a csv                              Writes the factors output as a csv
    """                                                             """
    output_path = Path(analysis_output, f"CIN_Census_factors.       output_path = Path(analysis_output, f"CIN_Census_factors.
    factors.to_csv(output_path, index=False)                        factors.to_csv(output_path, index=False)
                                                                
                                                                
def referral_inputs(flatfile):                                  def referral_inputs(flatfile):
    """                                                             """
    Creates three inputs for referral journeys analysis file        Creates three inputs for referral journeys analysis file
    """                                                             """
    ref = filter_flatfile(flatfile, filter="CINreferralDate")       ref = filter_flatfile(flatfile, filter="CINreferralDate")
    s17 = filter_flatfile(flatfile, filter="AssessmentActualS       s17 = filter_flatfile(flatfile, filter="AssessmentActualS
    s47 = filter_flatfile(flatfile, filter="S47ActualStartDat       s47 = filter_flatfile(flatfile, filter="S47ActualStartDat
    return ref, s17, s47                                            return ref, s17, s47
                                                                
                                                                
# FIXME: This function defaults to returning nothing - should <
#        or simply just returning the series and then doing ` <
def _time_between_date_series(later_date_series, earlier_date   def _time_between_date_series(later_date_series, earlier_date
    days_series = later_date_series - earlier_date_series           days_series = later_date_series - earlier_date_series
    days_series = days_series.dt.days                               days_series = days_series.dt.days
                                                                
    if days == 1:                                                   if days == 1:
        return days_series                                              return days_series
                                                                
    elif years == 1:                                                elif years == 1:
        years_series = (days_series / 365).apply(np.floor)              years_series = (days_series / 365).apply(np.floor)
        years_series = years_series.astype('Int32')                     years_series = years_series.astype('Int32')
        return years_series                                             return years_series
                                                                
                                                                
def _filter_event_series(dataset: pd.DataFrame, days_series:  | def _filter_event_series(dataset, days_series, max_days):
    """                                                       <
    Filters a dataframe to only include rows where the column <
    is 0 <= days_series <= max_days                           <
                                                                
    Args:                                                     <
        dataset (pd.DataFrame): The dataset to filter         <
        days_series (str): The name of the column containing  <
        max_days (int): The maximum number of days between th <
                                                              <
    Returns:                                                  <
        pd.DataFrame: The filtered dataset                    <
                                                              <
    """                                                       <
    dataset = dataset[                                              dataset = dataset[
        ((dataset[days_series] <= max_days) & (dataset[days_s           ((dataset[days_series] <= max_days) & (dataset[days_s
    ]                                                               ]
    return dataset                                                  return dataset
                                                                
                                                                
def merge_ref_s17(ref, s17, ref_assessment):                    def merge_ref_s17(ref, s17, ref_assessment):
    """                                                             """
    Merges ref and s17 views together, keeping only logically       Merges ref and s17 views together, keeping only logically
    """                                                             """
    # Merges referrals and assessments                              # Merges referrals and assessments
    ref_s17 = ref.merge(                                            ref_s17 = ref.merge(
        s17[["LAchildID", "AssessmentActualStartDate"]], how=           s17[["LAchildID", "AssessmentActualStartDate"]], how=
    )                                                               )
                                                                
    # Calculates days between assessment and referral               # Calculates days between assessment and referral
    ref_s17["days_to_s17"] = _time_between_date_series(             ref_s17["days_to_s17"] = _time_between_date_series(
        ref_s17["AssessmentActualStartDate"], ref_s17["CINref           ref_s17["AssessmentActualStartDate"], ref_s17["CINref
    )                                                               )
                                                                
    # Only assessments within config-specifed period followin       # Only assessments within config-specifed period followin
    ref_s17 = _filter_event_series(ref_s17, "days_to_s17", re       ref_s17 = _filter_event_series(ref_s17, "days_to_s17", re
                                                                
    # Reduces dataset to fields required for analysis               # Reduces dataset to fields required for analysis
    ref_s17 = ref_s17[["Date", "LAchildID", "AssessmentActual       ref_s17 = ref_s17[["Date", "LAchildID", "AssessmentActual
                                                                
    return ref_s17                                                  return ref_s17
                                                                
                                                                
def merge_ref_s47(ref, s47, ref_assessment):                    def merge_ref_s47(ref, s47, ref_assessment):
    """                                                             """
    Merges ref and s47 views together, keeping only logically       Merges ref and s47 views together, keeping only logically
    """                                                             """
    # Merges referrals and S47s                                     # Merges referrals and S47s
    ref_s47 = ref.merge(                                            ref_s47 = ref.merge(
        s47[["LAchildID", "S47ActualStartDate"]], how="left",           s47[["LAchildID", "S47ActualStartDate"]], how="left",
    )                                                               )
                                                                
    # Calculates days between S47 and referral                      # Calculates days between S47 and referral
    ref_s47["days_to_s47"] = _time_between_date_series(             ref_s47["days_to_s47"] = _time_between_date_series(
        ref_s47["S47ActualStartDate"], ref_s47["CINreferralDa           ref_s47["S47ActualStartDate"], ref_s47["CINreferralDa
    )                                                               )
                                                                
    # Only S47s within config-specifed period following refer       # Only S47s within config-specifed period following refer
    ref_s47 = _filter_event_series(ref_s47, "days_to_s47", re       ref_s47 = _filter_event_series(ref_s47, "days_to_s47", re
                                                                
    # Reduces dataset to fields required for analysis               # Reduces dataset to fields required for analysis
    ref_s47 = ref_s47[["Date", "LAchildID", "S47ActualStartDa       ref_s47 = ref_s47[["Date", "LAchildID", "S47ActualStartDa
                                                                
    return ref_s47                                                  return ref_s47
                                                                
                                                                
def ref_outcomes(ref, ref_s17, ref_s47):                        def ref_outcomes(ref, ref_s17, ref_s47):
    """                                                             """
    Merges views together to give all outcomes of referrals i       Merges views together to give all outcomes of referrals i
    Outcomes column defaults to NFA unless there is a relevan       Outcomes column defaults to NFA unless there is a relevan
    Calculates age of child at referral                             Calculates age of child at referral
    """                                                             """
    # Merge databases together                                      # Merge databases together
    ref_outs = ref.merge(ref_s17, on=["Date", "LAchildID"], h       ref_outs = ref.merge(ref_s17, on=["Date", "LAchildID"], h
    ref_outs = ref_outs.merge(ref_s47, on=["Date", "LAchildID       ref_outs = ref_outs.merge(ref_s47, on=["Date", "LAchildID
                                                                
    # Set default outcome to "NFA"                                  # Set default outcome to "NFA"
    ref_outs["referral_outcome"] = "NFA"                            ref_outs["referral_outcome"] = "NFA"
                                                                
    # Set outcome to "S17" when there is a relevant assessmen       # Set outcome to "S17" when there is a relevant assessmen
    ref_outs.loc[                                                   ref_outs.loc[
        ref_outs["AssessmentActualStartDate"].notnull(), "ref           ref_outs["AssessmentActualStartDate"].notnull(), "ref
    ] = "S17"                                                       ] = "S17"
                                                                
    # Set outcome to "S47" when there is a relevant S47             # Set outcome to "S47" when there is a relevant S47
    ref_outs.loc[ref_outs["S47ActualStartDate"].notnull(), "r       ref_outs.loc[ref_outs["S47ActualStartDate"].notnull(), "r
                                                                
    # Set outcome to "Both S17 & S47" when there are both           # Set outcome to "Both S17 & S47" when there are both
    ref_outs.loc[                                                   ref_outs.loc[
        (                                                               (
            (ref_outs["AssessmentActualStartDate"].notnull())               (ref_outs["AssessmentActualStartDate"].notnull())
            & (ref_outs["S47ActualStartDate"].notnull())                    & (ref_outs["S47ActualStartDate"].notnull())
        ),                                                              ),
        "referral_outcome",                                             "referral_outcome",
    ] = "Both S17 & S47"                                            ] = "Both S17 & S47"
                                                                
    # Calculate age of child at referral                            # Calculate age of child at referral
    ref_outs["Age at referral"] = _time_between_date_series(        ref_outs["Age at referral"] = _time_between_date_series(
        ref_outs["CINreferralDate"], ref_outs["PersonBirthDat           ref_outs["CINreferralDate"], ref_outs["PersonBirthDat
    )                                                               )
                                                                
    return ref_outs                                                 return ref_outs
                                                                
                                                                
def export_reffile(analysis_output, ref_outs):                  def export_reffile(analysis_output, ref_outs):
    """                                                             """
    Writes the referral journeys output as a csv                    Writes the referral journeys output as a csv
    """                                                             """
    output_path = Path(analysis_output, f"CIN_Census_referral       output_path = Path(analysis_output, f"CIN_Census_referral
    ref_outs.to_csv(output_path, index=False)                       ref_outs.to_csv(output_path, index=False)
                                                                
                                                                
def journey_inputs(flatfile):                                   def journey_inputs(flatfile):
    """                                                             """
    Creates inputs for the journey analysis file              |     Creates the input for the journey analysis file
    """                                                             """
    # Create inputs from flatfile and merge them                    # Create inputs from flatfile and merge them
    s47_j = filter_flatfile(flatfile, "S47ActualStartDate")         s47_j = filter_flatfile(flatfile, "S47ActualStartDate")
    cpp = filter_flatfile(flatfile, "CPPstartDate")                 cpp = filter_flatfile(flatfile, "CPPstartDate")
    return s47_j, cpp                                               return s47_j, cpp
                                                                
                                                                
def journey_merge(s47_j, cpp, icpc_cpp_days, s47_cpp_days):     def journey_merge(s47_j, cpp, icpc_cpp_days, s47_cpp_days):
    """                                                             """
    Merges inputs to produce outcomes file                          Merges inputs to produce outcomes file
    """                                                             """
    s47_cpp = s47_j.merge(                                          s47_cpp = s47_j.merge(
        cpp[["LAchildID", "CPPstartDate"]], how="left", on="L           cpp[["LAchildID", "CPPstartDate"]], how="left", on="L
    )                                                               )
                                                                
    # Calculate days from ICPC to CPP start                         # Calculate days from ICPC to CPP start
    s47_cpp["icpc_to_cpp"] = _time_between_date_series(             s47_cpp["icpc_to_cpp"] = _time_between_date_series(
        s47_cpp["CPPstartDate"], s47_cpp["DateOfInitialCPC"],           s47_cpp["CPPstartDate"], s47_cpp["DateOfInitialCPC"],
    )                                                               )
                                                                
    # Calculate days from S47 to CPP start                          # Calculate days from S47 to CPP start
    s47_cpp["s47_to_cpp"] = _time_between_date_series(              s47_cpp["s47_to_cpp"] = _time_between_date_series(
        s47_cpp["CPPstartDate"], s47_cpp["S47ActualStartDate"           s47_cpp["CPPstartDate"], s47_cpp["S47ActualStartDate"
    )                                                               )
                                                                
    # Only keep logically consistent events (as defined in co       # Only keep logically consistent events (as defined in co
    s47_cpp = s47_cpp[                                              s47_cpp = s47_cpp[
        ((s47_cpp["icpc_to_cpp"] >= 0) & (s47_cpp["icpc_to_cp           ((s47_cpp["icpc_to_cpp"] >= 0) & (s47_cpp["icpc_to_cp
        | ((s47_cpp["s47_to_cpp"] >= 0) & (s47_cpp["s47_to_cp           | ((s47_cpp["s47_to_cpp"] >= 0) & (s47_cpp["s47_to_cp
    ]                                                               ]
                                                                
    # Merge events back to S47_j view                               # Merge events back to S47_j view
    s47_outs = s47_j.merge(                                         s47_outs = s47_j.merge(
        s47_cpp[["Date", "LAchildID", "CPPstartDate", "icpc_t           s47_cpp[["Date", "LAchildID", "CPPstartDate", "icpc_t
        how="left",                                                     how="left",
        on=["Date", "LAchildID"],                                       on=["Date", "LAchildID"],
    )                                                               )
                                                                
    return s47_outs                                                 return s47_outs
                                                                
                                                                
def s47_paths(s47_outs, s47_day_limit, icpc_day_limit):         def s47_paths(s47_outs, s47_day_limit, icpc_day_limit):
    """                                                             """
    Creates an output that can generate a Sankey diagram of o       Creates an output that can generate a Sankey diagram of o
    """                                                             """
    # Dates used to define window for S47 events where outcom       # Dates used to define window for S47 events where outcom
    for y in s47_outs["YEAR"]:                                      for y in s47_outs["YEAR"]:
        s47_outs["cin_census_close"] = datetime(int(y), 3, 31           s47_outs["cin_census_close"] = datetime(int(y), 3, 31
    s47_outs["s47_max_date"] = s47_outs["cin_census_close"] -       s47_outs["s47_max_date"] = s47_outs["cin_census_close"] -
        s47_day_limit                                                   s47_day_limit
    )                                                               )
    s47_outs["icpc_max_date"] = s47_outs["cin_census_close"]        s47_outs["icpc_max_date"] = s47_outs["cin_census_close"] 
        icpc_day_limit                                                  icpc_day_limit
    )                                                               )
                                                                
    # Setting the Sankey diagram source for S47 events              # Setting the Sankey diagram source for S47 events
    step1 = s47_outs.copy()                                         step1 = s47_outs.copy()
    step1["Source"] = "S47 strategy discussion"                     step1["Source"] = "S47 strategy discussion"
                                                                
    # Setting the Sankey diagram destination for S47 events         # Setting the Sankey diagram destination for S47 events
    step1["Destination"] = np.nan                                   step1["Destination"] = np.nan
                                                                
    step1.loc[step1["DateOfInitialCPC"].notnull(), "Destinati       step1.loc[step1["DateOfInitialCPC"].notnull(), "Destinati
                                                                
    step1.loc[                                                      step1.loc[
        step1["DateOfInitialCPC"].isnull() & step1["CPPstartD           step1["DateOfInitialCPC"].isnull() & step1["CPPstartD
        "Destination",                                                  "Destination",
    ] = "CPP start"                                                 ] = "CPP start"
                                                                
    step1.loc[                                                      step1.loc[
        (                                                               (
            (step1["Destination"].isnull())                                 (step1["Destination"].isnull())
            & (step1["S47ActualStartDate"] >= step1["s47_max_               & (step1["S47ActualStartDate"] >= step1["s47_max_
        ),                                                              ),
        "Destination",                                                  "Destination",
    ] = "TBD - S47 too recent"                                      ] = "TBD - S47 too recent"
                                                                
    step1.loc[step1["Destination"].isnull(), "Destination"] =       step1.loc[step1["Destination"].isnull(), "Destination"] =
                                                                
    # Setting the Sankey diagram source for ICPC events             # Setting the Sankey diagram source for ICPC events
    step2 = step1[step1["Destination"] == "ICPC"]                   step2 = step1[step1["Destination"] == "ICPC"]
    step2["Source"] = "ICPC"                                        step2["Source"] = "ICPC"
                                                                
    # Setting the Sankey diagram destination for ICPC events        # Setting the Sankey diagram destination for ICPC events
    step2["Destination"] = np.nan                                   step2["Destination"] = np.nan
                                                                
    step2.loc[step2["CPPstartDate"].notnull(), "Destination"]       step2.loc[step2["CPPstartDate"].notnull(), "Destination"]
                                                                
    step2.loc[                                                      step2.loc[
        (                                                               (
            (step2["Destination"].isnull())                                 (step2["Destination"].isnull())
            & (step2["DateOfInitialCPC"] >= step2["icpc_max_d               & (step2["DateOfInitialCPC"] >= step2["icpc_max_d
        ),                                                              ),
        "Destination",                                                  "Destination",
    ] = "TBD - ICPC too recent"                                     ] = "TBD - ICPC too recent"
                                                                
    step2.loc[step2["Destination"].isnull(), "Destination"] =       step2.loc[step2["Destination"].isnull(), "Destination"] =
                                                                
    # Merge the steps together                                      # Merge the steps together
    s47_journey = pd.concat([step1, step2])                         s47_journey = pd.concat([step1, step2])
                                                                
    # Calculate age of child at S47                                 # Calculate age of child at S47
    s47_journey["Age at S47"] = _time_between_date_series(          s47_journey["Age at S47"] = _time_between_date_series(
        s47_journey["S47ActualStartDate"], s47_journey["Perso           s47_journey["S47ActualStartDate"], s47_journey["Perso
    )                                                               )
                                                                
    return s47_journey                                              return s47_journey
                                                                
                                                                
def export_journeyfile(analysis_output, s47_journey):           def export_journeyfile(analysis_output, s47_journey):
    """                                                             """
    Writes the S47 journeys output as a csv                         Writes the S47 journeys output as a csv
    """                                                             """
    output_path = Path(analysis_output, f"CIN_Census_S47_jour       output_path = Path(analysis_output, f"CIN_Census_S47_jour
    s47_journey.to_csv(output_path, index=False)                    s47_journey.to_csv(output_path, index=False)

```
# Data Specification: CIN Census

Test Coverage: 10%

Three CLI options:

* cleanfile(input, la_code, la_log_dir, output)
* la_agg(input, flat_output, analysis_output)
* pan_agg(input, la_code, flat_output, analysis_output)


## CLI COMMAND: Cleanfile

* cleanfile
    * check_file_type [shared] - check if input file is supported (REQ AASUPTYPE)
        * Uses pathlib to get the file stem and suffix
        * Returns None if suffix is an exact match (case sensitive) of one of the `file_types`
        * Raises AssertionError if suffix is one of the `supported_file_types` (but not one of the `file_types`)
        * If neither of those conditions are met:
            * writes a warning to a dynamic file name **DAGSTER WARNING**
            * returns the string "incorrect file type"

        * the return value of this is now checked and if the value is "incorrect file type" then the pipeline is stopped

        * **NEEDS REWRITE FOR DAGSTER**

    * dom_parse **DAGSTER WARNING**

    * list(stream) - loads stream to memory ?!?

    * filename from inputfile using Path.stem

    * check_year(filename) - get year from filename
        * Supports a terrifying number of year formats - is there a better way?

    * ON (AttributeError, ValueError): save_year_error - EXIT **DAGSTER WARNING**

    * years_to_go_back / year_start_month - BOTH HARDCODED TO 6

    * check_year_within_range
        * if False: save_incorrect_year_error - EXIT **DAGSTER WARNING**

    * Config()
        - Tries very hard to be configurable - but doesn't seem to be used?
        - Uses environment variables to set the config - do we need this?

    * flip_dict - see warnings

    * strip_text
        * Removes leading and trailing whitespace from TextNode events as well as removing empty strings alltogether

    * add_context
        * Adds the context 'path' to each event

    * add_schema
        * Adds the schema to each event
        - WARNING: Late failure if schema for year is not found

    * inherit_LAchildID
        * Caches each <Child></Child> series and attempts to find the LAchildID from the cached series
        - WARNING: Inconsistent try/catch could lead to unpredictable results

    * validate_elements
        * Validates the local context and adds validation errors
        - This is presumably quite costly and repeats a lot of validation as it validates at each level of the tree
        
        * _get_validation_error
            * Tries to extract the validation information from the error message
            - WARNING: This is very fragile and will break if the error message changes when ET/lxml changes
            - a lot of nested ifs - very difficult to understand

    * counter
        * Counts validation errors - adds these to shared context

    * convert_true_false
        * Converts text nodes 'true' and 'false' to "0" and "1" based on schema

    * remove_invalid
        * Removes subtrees if they are not valid and the list is on a hardcoded list of tags
        - Q: List of tags is passed into this function - but it's hardcoded - if it *HAS* to be hardcoded, could it at least sit in this function rather than
          in the CLI code which now requires duplication?
        - Believe this is quite inefficient due to the use of collector which will inspect multiple sub-trees (potentially)
        - Several of these functions would be so much simpler using DOM instead of stream

    * message_collector
        * Collects Header and CINEvent
        - We know that this is confusing and difficult to follow - and should hopefully be possible to replace with `sfdata` once complete

    * export_table
        * uses event_to_records to convert the stream to tablib

    * add_fields - transforms - WARNING: Misleading name
        
        * convert_to_dataframe
            * Just an alias for tablib.export('df') - could be removed

        * get_year - sets the year column - WARNING: Misleading name
            * Just an alias for dataframe['column'] = value

        
        * convert_to_datetime 
            * hardcoded list of columns to convert to datetime - no error handling

        * add_school_year
            * applies _get_person_school_year using hardcoded column names

            * _get_person_school_year
                * Returns year or None - although I'm not sure when none could be returned

        * add_la_name
            * Another one liner: `dataframe['LA'] = la_name`

        * la_prefix
            * `data["LAchildID"] = data["LAchildID"] + "_" + la_code`

        * degrade_dob
            * Uses hardcoded column name to apply `to_month_only_dob`

        * degrade_expected_dob
            * Repetition of above

        * degrade_death_date
            * Repetition of above -  love that death_date calls dob function
            
        * export_file
            * Hardcoded output file name
            * Uses dataframe.to_csv **DAGSTER WARNING**

        * save_errors_la
            * Uses open **DAGSTER WARNING**

## CLI COMMAND: la_agg

* la_agg

    * Config()

    * read_file - `pd.read_csv(input, parse_dates=dates, dayfirst=True)` **DAGSTER WARNING**

    * merge_la_files
        * Reads the 'archive' file and merges it with the 'current' file
        * Hardocded filename
        * Uses pd.read_csv **DAGSTER WARNING**

    * deduplicate
        * Removes duplicate entries based on primary keys

    * remove_old_data
        * Removes data older than six years
        * June hardcoded as month
        * "today" hardcoded as reference - different from AA which uses "now"
        * `year` and `years` as argument names are very dangerous - must use more descriptive names

    * export_flatfile
        * Writes the dataframe to a csv file - alias for `dataframe.to_csv` **DAGSTER WARNING**
        * Hardcoded filename - should at least be constant

    * filter_flatfile
        * Filters the dataframe based on column 'Type' == 'AssessmentAuthorisationDate' and drops columns that now are empty

    * IF len(factors) > 0 - factors is output of filter_flatfile
        * split_factors
            * If I understand this correctly, there is a column called Factors which has a list of strings in it. This 
              function translates this column to a One-Hot encoded vuew with a columns for each factor.

        * export_factfile
            * Writes the dataframe to a csv file - alias for `dataframe.to_csv` **DAGSTER WARNING**
            * Hardcoded filename

    * referral_inputs
        * Returns a tuple of three dataframes all having been individually filtered by filter_flatfile
        * ref = "CINreferralDate", s17 = "AssessmentActualStartDate", s47 = "S47ActualStartDate"

    * IF len(s17) AND len(s47) - s17 and s47 are output of referral_inputs and both must have values to proceed

        * merge_ref_s17
            * Merges the two dataframes on the 'LAchildID'
            * Calculates the dates between AssessmentActualStartDate and CINreferralDate

        * merge_ref_s47
            * Merges the two dataframes on the 'LAchildID'
            * Calculates the dates between S47ActualStartDate and CINreferralDate
            - *This is identical to merge_ref_s17 just acting on different columns*

        * ref_outcomes
            - **Inputs**:
                - `ref`: Primary dataframe.
                - `ref_s17`: Dataframe for S17 outcomes.
                - `ref_s47`: Dataframe for S47 outcomes.

            - **Operations**:
                - Merge `ref` with `ref_s17` based on `Date` and `LAchildID`.
                - Merge the resulting dataframe with `ref_s47` based on the same keys.
                - Set a default outcome in a column named `referral_outcome` to "NFA" for all records.
                - Change outcome to "S17" when an `AssessmentActualStartDate` is present.
                - Change outcome to "S47" when a `S47ActualStartDate` is present.
                - Set outcome to "Both S17 & S47" when both start dates are present.
                - Calculate age of the child at the time of referral using the function `_time_between_date_series()` and store in `Age at referral`.

            - **Output**:
                - Dataframe (`ref_outs`) containing merged views with outcomes and child's age at referral. 

        * export_reffile
            * Saves the merged file to a csv file - alias for `dataframe.to_csv` **DAGSTER WARNING**

    * journey_inputs
        * Returns a tuple of two dataframes all having been individually filtered by filter_flatfile
        * s47_j = "S47ActualStartDate", cpp = "CPPstartDate"

    * IF len(s47_j) AND len(cpp) - s47_j and cpp are output of journal_inputs and both must have values to proceed

        * journey_merge
            - Merge `s47_j` with `CPPstartDate` from `cpp` based on `LAchildID` to get `s47_cpp`.
            - Calculate days from ICPC to CPP start:
                - Add a new column `icpc_to_cpp` to `s47_cpp`.
                - Use helper function `_time_between_date_series` to calculate the days difference.
            - Calculate days from S47 to CPP start:
                - Add a new column `s47_to_cpp` to `s47_cpp`.
                - Use helper function `_time_between_date_series` to calculate the days difference.
            - Filter `s47_cpp` to keep only logically consistent events:
                - Based on constraints defined for `icpc_to_cpp` and `s47_to_cpp` using the config variables `icpc_cpp_days` and `s47_cpp_days`.
            - Merge filtered events from `s47_cpp` back to `s47_j` to get `s47_outs`:
                - Keep columns ["Date", "LAchildID", "CPPstartDate", "icpc_to_cpp", "s47_to_cpp"].
                - Merge based on ["Date", "LAchildID"].
            - Return `s47_outs`.

        * s47_paths
            - **Purpose**: Creates an output that can generate a Sankey diagram of outcomes from S47 events.
            
            - **Step 1: Define Date Window for S47 events**
                - For each year in `s47_outs["YEAR"]`:
                    - Define the date for the 'cin_census_close' as March 31st of that year.
                - Compute the 's47_max_date' by subtracting the 's47_day_limit' from the 'cin_census_close'.
                - Compute the 'icpc_max_date' by subtracting the 'icpc_day_limit' from the 'cin_census_close'.

            - **Step 2: Setting up the Sankey diagram source for S47 events**
                - Create a copy of `s47_outs` named `step1`.
                - Set the "Source" column values to "S47 strategy discussion".
                - Initialize the "Destination" column with NaN values.
                - Update the "Destination" for rows where 'DateOfInitialCPC' is not null to "ICPC".
                - Update the "Destination" for rows where 'DateOfInitialCPC' is null but 'CPPstartDate' is not null to "CPP start".
                - Update the "Destination" for rows where 'S47ActualStartDate' is on or after 's47_max_date' to "TBD - S47 too recent".
                - For remaining rows with null "Destination", set the value to "No ICPC or CPP".

            - **Step 3: Setting up the Sankey diagram source for ICPC events**
                - Filter `step1` where the "Destination" is "ICPC" and assign to `step2`.
                - Set the "Source" column values of `step2` to "ICPC".
                - Initialize the "Destination" column with NaN values.
                - Update the "Destination" for rows where 'CPPstartDate' is not null to "CPP start".
                - Update the "Destination" for rows where 'DateOfInitialCPC' is on or after 'icpc_max_date' to "TBD - ICPC too recent".
                - For remaining rows with null "Destination", set the value to "No CPP".

            - **Step 4: Merge the steps together**
                - Concatenate `step1` and `step2` into `s47_journey`.

            - **Step 5: Calculate Age of Child at S47**
                - Compute the child's age at the time of the S47 event by finding the difference between 'S47ActualStartDate' and 'PersonBirthDate' in terms of years.

            - **Return**: The function finally returns the `s47_journey` dataframe.        

        * export_journeyfile
            * Saves the merged file to a csv file - alias for `dataframe.to_csv` **DAGSTER WARNING**

## CLI COMMAND: pan_agg

* pan_agg

    * Config

    * read_file
        * Reads the file from the filepath
        * Alias for `pandas.read_csv` **DAGSTER WARNING**

    * merge_agg_files
        * Reads the pan flatfile using pandas.read_csv **DAGSTER WARNING**
        
        * _merge_dfs 
            * Drops the LA column
            * Merges the new columns to the pan flatfile
            * *ONLY CALLED FROM PARENT* - should probably be inline

    * export_flatfile
        * Saves the merged file to a csv file - alias for `dataframe.to_csv` **DAGSTER WARNING**


    * filter_flatfile

    * At this stage it pretty much exactly follows the steps from la_agg

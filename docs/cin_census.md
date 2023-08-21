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

    * **INCOMPLETE**

## CLI COMMAND: pan_agg

* pan_agg
    * 
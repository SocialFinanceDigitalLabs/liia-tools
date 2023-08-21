# Data Specification: Annex A

Three CLI options:

* cleanfile(input, la_code, la_log_dir, output) - Cleans input Annex A xlsx files according to config and outputs cleaned xlsx files
* la_agg(input, output) - Joins data from newly cleaned Annex A file (output of cleanfile()) to existing Annex A data for the depositing local authority
* pan_agg(input, la_code, output) - Merges data from newly merged Annex A file (output of la_agg()) to existing pan-London Annex A data


KWS: I have a some questions about the configuration mechanism. I believe it may originate in some code Celine and I developed to simplify processing
in Jupyter notebooks and have maintained the same approach. The key thing about that configuration is that it was NOT standardised and the 
user (notebook developer) could override the defaults. As this is not supported by the pipeline, I think it adds a lot of unnecessary complexity
as well as means these pipelines are unneccesarily interdependent. Releasing a small change to one pipeline means the entire set need to be retested
due to the up-down nature of the configuration:

* liiatools
  * datasets
    * annex_a
  * spec
    * annex_a

Because of this, package names are also unnecessarily long and therefore relie on abbreviations making it confusing for reviewers and future developers.

A simpler approach would be to have each pipeline as a standalone package with a single configuration file, e.g.

* liiatools_annex_a
  * clean
  * local_authority_aggregation
  * pan_london_aggregation
  * config

This could then be versioned independently and released independently allowing for simpler change management and less testing required.


## CLI COMMAND: Cleanfile

* cleanfile
    * Config()
        - Tries very hard to be configurable - but doesn't seem to be used?
        - Uses environment variables to set the config - do we need this?

    * la_name - looks up name based on code
       - **WARNING**: uses a function called `flip_dict` which does not check for duplicate values - so could fail silently
        
    * check if input file is supported (REQ AASUPTYPE)
    * read file using `parse_sheets` from sfdata_stream_parser
        - **Uses open - DAGSTER WARNING**
    * configure stream (`clean_config.configure_stream`)
        * Uses: config["datasources"] and config["data_config"]
        * identify_blank_rows (REQ AABLANKROW)
            - Q: Why aren't these removed here as it would massively speed up the rest of the process?
        * add_sheet_name (REQ AASHEETNAME)
        * Add property `sheet_name` to all child events
        * Add property `column_headers` to all child events
        * Add property `column_header` based on the column index and the `column_headers` property
        * Modified property `column_header` by trying to match it to a known header
            - Q: Some slightly confusing code here - e.g. forcing the existing value to string when we have already set this
            - Silently ignores any errors
        * Looks up sheet & column in config["data_config"] and adds this to property `category_config`
        * Looks up sheet & column in config["datasources"] and adds this to property `other_config`

    * clean stream (`cleaner.clean`)
        * clean_cell_category
        * clean_integers
            * to_integer [local] - some strange code here - also returns empty string if it can't convert to integer
        * clean_dates
            * to_date [shared] - very strict date format - also may not support excel dates if not formatted
        * clean_postcodes
            * check_postcode [shared] - Uses a simplified postcode regex - ignores case but does not uppercase 
            - Check runs based on header name rather than type like the others

    * degrade
        * degrade_postcodes
            * Check runs based on header name rather than type like the others
            * to_short_postcode [shared]
                - Uses a simplified postcode regex - ignores case 
                - By this stage postcodes should already be correctly formatted - so don't need the regex
                - Fails silently

        * degrade_dob
            * Check runs based on header name rather than type like the others
            * to_month_only_dob [shared]
                - Replaces day with 01
                - Returns empty string if it can't convert to date
                - Misleading name

    * log_errors
        * create_error_table
            - Emits an ErrorTable event **INSTEAD OF** the EndTable event (**EndTable is removed**)
        * blank_error_check
            - Checks if 'required' (not `canbeblank`) fields are filled
        * create_error_list
            - `formatting_error`
            - Sets a property on the ErrorTable event with a collected set of errors
            - The errors are just the column_headers where the 'error_name' is "1"
        * create_error_list 
            - `blank_error`
            - Sets a property on the ErrorTable event with a collected set of errors
            - The errors are just the column_headers where the 'error_name' is "1"
            - As this is just running code over the `blank_error_check` property - this could be done in one step
        * inherit_error
            - `extra_columns`
            - Difficult to read, but think it copies the error_name from the starttable to the following error_table
            - Not quite sure exactly what this does, but believe there are sequence bugs here
        * duplicate_column_check
            * _duplicate_columns
                - Works - but quite verbose - could use the library https://iteration-utilities.readthedocs.io/en/latest/generated/duplicates.html 
                  which is one of the fastests implementations around
            - For some reason converts this list to a long descriptive string - would be better done at output
            - Uses a hacky conversion to string, rather than join
        * inherit_error
            - `duplicate_columns`
            - Could this not just be done as part of the previous step?
        * create_file_match_error
            - Uses try/except to check if `sheet_name` property exists - and adds long error message if not
        * create_missing_sheet_error
            - Checks against hardcoded list of sheet names rather than load from config and adds long error to error table event
            - Would fail on multiple containers in single file - unlikely to happen but still bug

    * create_la_child_id
        * Adds the LA code to the end of the existing child or adopter ids.
        - Q: This is just preference - but why at the end rather than start? It would mean they sorted nice and follows the logical hierarchy of the data.

    * save_stream
        * coalesce_row
            * Creates a dict and **REPLACES** StartRow and EndRow with a RowEvent
            - known cells are also removed, but unknown cells are kept potentially creating quite a confusing stream after this
        * filter_rows
            * Hardcoded list of sheets to remove - but doesn't remove them, only flags filter=0 or filter=1 (this time uses int not string)
        * create_tables
            * Condences the stream into a TableEvent holding a tablib version of the data
            - REMOVES StartTable events
            - Yields EndTable events and TableEvents
            - If RowEvent and filter == 0 then adds to the table and REMOVES the RowEvent, otherwise yields the RowEvent
        * save_tables
            - StartContainer - create new DataBook
            - EndContainer - save DataBook to file using open and hardcoded filename pattern
            - StartTable - set the sheet_name - **StartTable IS REMOVED BY THIS STAGE?**
            - TableEvent with data - add to DataBook
            - **Uses open - DAGSTER WARNING**

    * save_errors_la
        * Creates an error file 
        - This function is so complex I don't think I can understand it
        - Also can't be unit tested - and is not tested in the test suite
        - Uses a hardcoded filename pattern
        - **Uses open - DAGSTER WARNING**



Requirement | Description
--- | ---
AASUPTYPE | Must be one of xlsx or xlsm - however, supports also .xml and .csv? - check if this is correct | 
AABLANKROW | Blank rows are flagged with `blank_row="1"`
AASHEETNAME | Match the loaded table against one of the Annex A sheet names using fuzzy matching with regex
AACAT | Clean categories
AAINT | Clean integers
AADATE | Clean dates
AAPCODE | Clean postcodes
AADEGPCODE | Degrade the postcode column
AACHILD_ID | Adds the Local Authority code to the Child Unique ID / Individual adopter identifier to create a unique identifier for each child


## CLI COMMAND: la_agg

* la_agg
    * Config()
        - Uses environment variables to set the config - do we need this?

    * split_file
        * Reads the input file into a dict of sheet_name: dataframe
        - just an alias for pd.read_excel **DAGSTER WARNING**
        - could just as well call read_excel directly?

    * sort_dict
        * Sorts the sheets by config['sort_order']
        - **WARNING** - Python dicts are not ordered - so this is not guaranteed to work - from Python 3.6 onwards CPython dicts are ordered - but this is not a language feature

    * merge_la_files
        * **Reads** hardcoded file name from **output** - this is confusing naming? 
        * Calls pd.read_excel **DAGSTER WARNING**
        - Fails silently if file does not exist
        
        * _merge_dfs
            * merges original file (from split_file) with the output file using pd.concat
            * **WARNING**: If a sheet does not exist in the input file - it will not be created in the output file and any existin records dropped - bug?

    * deduplicate
        * Uses config["dedup"] - this is a list of the primary key columns for each table
        * Calls df.drop_duplicates on each table using the primary key columns from the config
    
    * convert_datetimes
        * Uses config["dates"] - this is a list of the date columns for each table
        - Calls pd.to_datetime on each table using the date columns from the config **uses hardcoded date format**


    * remove_old_data
        * Takes index_date from config file (**DAGSTER WARNING - this date must be configurable**)
        - Uses hardcoded table and column names
        - Does some pandas magic that is difficult to follow - but silently swallows errors so could be dangerous
        - **WARNING** As this is a IG protection function I would prefer it to be clearer and more explicit as well as well tested. The function 
          itself has 100% code coverage, but it depends on the `_remove_years` function which fails silently and the fail condition is not tested.

    * convert_dates
        * Uses config["dates"] - this is a list of the date columns for each table
        - This is a duplicate of `convert_datetimes` (**DRY**) with the addition of `.dt.date` on each column

    * export_file
        * Exports the sheets to an excel file using pd.ExcelWrite (**DAGSTER WARNING**)
        - Uses hardcoded filename - in fact the same hardcoded one as the input ouput file but without a constant
        - Suggest the sorting of sheets could happen here as a list rather than the sorted dict above

    
There is no logging or sanity checks performed by this code. The modifications are relatively simple, but there are still obvious things that could go wrong.
The overwriting of the input file means that if a run fails, the original file is lost with no rollback mechanism.

There is also a clear race condition where a second run starts before another is complete - this would lead to the loss of session data from whichever run finishes first.

These are probably minor issue, but nonetheless without debugging or logging it would be difficult to diagnose or even be aware of any issues.

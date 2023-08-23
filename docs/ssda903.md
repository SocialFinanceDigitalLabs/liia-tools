# Data Specification: SSDA903 Return

Test Coverage: 71%

Structure here is slightly different in that the CLI just calls functions in `s903_main_functions.py`. I'm not sure why this is the case.

Four CLI options:

* cleanfile(input, la_code, la_log_dir, output)
* la_agg(input, output)
* pan_agg(input, la_code, output)
* suffiency_output(input, output)

**QUESTIONS**
    * Part of this process is to filter out tables that should not be shared. However, nothing removes these files if they already exist at source. Is this a problem?


## CLI COMMAND: Cleanfile

* cleanfile
    * delete_unrequired_files
        * takes a list of filenames to delete
        * loops over each configured filename, and if the current filename matches the configured filename, deletes the input file
        * save_unrequired_file_error
            * Opens a dyanmic filaneme based on the input filename **DAGSTER WARNING**
        - **Q:** Why doesn't this just check if the input filename is IN the filelist? 

    * check_blank_file
        * Attempts to open the input file using pd.read_csv **DATSTER WARNING**
        * If opening the files raises pandas.errors.EmptyDataError:
            * Opens a dynamic filename based on the input filename **DAGSTER WARNING**
            * **RETURNS**: "empty" (str)

    * IF the previous return == "empty", exits operation

    - **NOTE**: Although if successful the above file has now been read as a dataframe, it is not kept

    * drop_empty_rows
        * Opens the input file using pd.read_csv **DAGSTER WARNING**
        * Drops any rows that are completely empty
        * Saves the file to the output filename using df.to_csv **DAGSTER WARNING**

    * Calls the `check_year` function to get the year from the filename (see comments in [cin_census.md](cin_census))
        * if the above fails, calls `save_year_error` which writes a single line to a dynamically named logfile **DAGSTER WARNING**
    
    * Calls `check_year_within_range` function to check if file should be processed (see comments in [cin_census.md](cin_census))
        * if the above fails, calls `save_incorrect_year_error` which writes a single line to a dynamically named logfile **DAGSTER WARNING**

    * Calls `check_file_type` function to check if file should be processed (see comments in [cin_census.md](cin_census))
        * this function is a bit more complex, and writes the logfile itself **DAGSTER WARNING**
    
    - **FROM HERE WE SWITCH FROM PANDAS TO STREAM**
    
    * parse_csv
        * Uses tablib to open the input file **DAGSTER WARNING**
        * Returns stream of table events

    * add_year_column

    * configure_stream
        * add_table_name
        * inherit_property(table_name)
        * inherit_property(expected_columns) **NOTE** This is set by `add_table_name`
        * match_config_to_cell

    * clean
        * clean_dates
        * clean_categories
        * clean_integers
        * clean_postcodes

    * degrade
        * degrade_postcodes
        * degrade_dob

    * log_errors
        * blank_error_check
        * create_formatting_error_count
        * create_blank_error_count
        * create_file_match_error
        * create_extra_column_error

    * create_la_child_id

    * save_stream
        * coalesce_row
        * create_tables
        * save_tables

    * save_errors_la


## CLI COMMAND: la_agg

* la_agg

    * Config()

    * read_file - alias for pd.read_csv **DAGSTER WARNING**

    * match_load_file
        * determines the current table name based on exactly mathing the column headers

    * merge_la_files
        * Uses the table name to look for a file with the name `SSDA903_{table_name}_merged.csv` **DAGSTER WARNING**
        * If this file exists - opens and merges it with the current dataframe

    * convert_datetimes
        * Finds the expected date columns for the current table_name
        * Converts these columns to datetime format using fixed format "%Y/%m/%d"
        - NOTE: Would be more efficient if it did all columns in one go

    * deduplicate
        * Sorts and drops duplicates based on the list of primary keys from the config file

    * remove_old_data
        The function `remove_old_data` is designed to filter out rows from a dataframe based on the "YEAR" column value, removing any rows where the "YEAR" is older than a specified number of years as at a given reference date.

        Here's a breakdown of what the function does:

        1. **Parameters:**
        - `s903_df`: This is a pandas DataFrame, presumably containing data from a CSV file, and there's a column named "YEAR" that has year values in it.
        - `num_of_years`: This specifies how many years of data you want to retain, counting back from a given reference date.
        - `new_year_start_month`: This denotes the month which signifies the start of a new year for the data retention policy. It allows the function to handle cases where the start of the "data year" is not necessarily January.
        - `as_at_date`: This is the reference date against which the valid range is determined. 

        2. **Calculate Current Year and Month:** The function extracts the year and month from the `as_at_date` using pandas' `to_datetime` function.

        3. **Determine the Earliest Allowed Year:** 
        - If the current month is before the specified `new_year_start_month`, then the `earliest_allowed_year` is simply the current year minus `num_of_years`.
        - Otherwise, it rolls forward one year by subtracting `num_of_years` from the current year and adding 1. This is to account for scenarios where, let's say, the data year starts in July and the `as_at_date` is in August. In that case, the current data year is considered to be in the retention period.

        4. **Filter the DataFrame:** The function then filters the input dataframe `s903_df` to retain only the rows where the "YEAR" column is greater than or equal to the `earliest_allowed_year`.

        5. **Return the Filtered DataFrame:** Finally, the filtered dataframe is returned.

        ### Example Usage:

        Consider you have a dataframe (`s903_df`) like this:
        ```
        YEAR  VALUE
        0  2019     10
        1  2020     20
        2  2021     30
        3  2022     40
        ```
        And your data retention policy is such that the data year starts in July, and you want to keep only the last 2 years of data as of August 2022.

        If you call the function like this:
        ```python
        filtered_df = remove_old_data(s903_df, 2, 7, "2022-08-01")
        ```
        The function would return a dataframe like this:
        ```
        YEAR  VALUE
        1  2020     20
        2  2021     30
        3  2022     40
        ```
        This means the function has removed data from 2019 since it's older than 2 years based on the retention policy starting from July.

    * IF FILE STILL HAS DATA

        * convert_dates
            * Same as `convert_datetimes` with the addition of `.dt.date`

        * export_la_file
            * Saves the dataframe to a CSV file using df.to_csv **DAGSTER WARNING**


## CLI COMMAND: pan_agg

* pan_agg

    * Config()

    * read_file - alias for pd.read_csv **DAGSTER WARNING**

    * match_load_file
        * DUPLICATE of `match_load_file` in `la_agg`

    * IF current file is a table to be kept (from config):

        * merge_agg_files
            * Loads the old pan london file **DAGSTER WARNING**

            * _merge_dfs

                * Drops old entries by LA code
                * Loads the current file
        
        * export_pan_file

            * Saves the dataframe to a CSV file using df.to_csv **DAGSTER WARNING**


## CLI COMMAND: sufficiency_output

* sufficiency_output

    * Config()

    * read_file - alias for pd.read_csv **DAGSTER WARNING**

    * match_load_file
        * DUPLICATE of `match_load_file` in `la_agg`

    * IF current file is a table to be kept (from config):

        * data_min
            * Removes columns specified in config
            - **Q:** Principles of data minimisation would say that one should specify columns to be preserved
        
        * export_suff_file
                * Saves the dataframe to a CSV file using df.to_csv **DAGSTER WARNING**
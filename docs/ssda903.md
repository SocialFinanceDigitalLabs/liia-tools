# Data Specification: SSDA903 Return

Test Coverage: 71%

Structure here is slightly different in that the CLI just calls functions in `s903_main_functions.py`. I'm not sure why this is the case.

Four CLI options:

* cleanfile(input, la_code, la_log_dir, output)
* la_agg(input, output)
* pan_agg(input, la_code, output)
* suffiency_output(input, output)


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

    * merge_la_files

    * convert_datetimes

    * deduplicate

    * remove_old_data

    * IF FILE STILL HAS DATA

        * convert_dates

        * export_la_file


## CLI COMMAND: pan_agg

* pan_agg

    * Config()

    * read_file - alias for pd.read_csv **DAGSTER WARNING**

    * match_load_file

    * IF current file is a table to be kept (from config):

        * merge_agg_files
        
        * export_pan_file


## CLI COMMAND: sufficiency_output

* sufficiency_output

    * Config()

    * read_file - alias for pd.read_csv **DAGSTER WARNING**

    * match_load_file

    * IF current file is a table to be kept (from config):

        * data_min
        
        * export_suff_file
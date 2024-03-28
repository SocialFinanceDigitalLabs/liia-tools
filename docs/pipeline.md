# General Pipeline

This general outline documents the steps that most of these pipelines will follow. For some pipelines, some steps may be skipped or additional steps may be added, however, by sticking to these general steps we can ensure that the data is processed in a consistent way, as well as making it easier to maintain the codebase

The high-level steps can be summarised as follows:

1. Prepfile - move file and collect metadata
2. Cleanfile - ensure file is in a consistent format
3. Enrich data - add additional data from other sources, e.g. LA or year
4. Apply Privacy Policy - degrade data to meet data minimisation rules
5. History 1 - Archive data
6. History 2 - Rollup data
7. Client reports
8. Prepare shareable reports
9. Data Retention 1 - Clear old history data
10. Data Retention 2 - Clear old session data
11. Data Retention 3 - Clear incoming data

But we will go into more detail below.

For all of these, there will be an 'incoming' file area, where the files are uploaded to, and a 'processed' file area, where the files are moved to after processing. In addition, there is a 'session' folder which is only visible to the pipeline but can be accessed by technical staff in case of troubleshoothing. 

To allow for data sharing, there is also a 'shareable' folder, which is a copy of the processed data appropriately minimised for sharing. This folder can only be seen by the client but also accessed by central pipelines for merging with other shared data.


## Prepfile

Initial setup & configuration including creating a new session folder, moving the incoming file to the session folder, and collecting metadata.

Expect to be a standard task to cover all pipelines.

If no new files are found, this step simply exits.

Returns:
* Session folder
* List of files
* Metadata - if provided in incoming folder, such as folder name

Questions:
* What if there are multiple folders? E.g. say we use folder per-year, but what if there are multiple files accross several years, do we run this multiple times or do we handle multiple years?

## Cleanfile

* Reads and parses the incoming files. 
* Detects the year if the format is year dependent.
* Ensures that the data is in a consistent format and that all required fields are present. 
* Creates "issues" lists of any quality problems identified such as:
  * Unknown files
  * Missing fields
  * Unknown fields
  * Incorrectly formatted data / categories
  * Missing data
* Creates dataframes for the indetified tables

Inputs:
  * Session folder

Outputs:
  * Dataframes for each table
  * Issues lists (format TBC)
  * Relevant metadata

Recommend that return object follows a standard format so it can be serialized in a standard way for all pipelines

## Enrich data

Adds standard enrichments to the data, these include:

  * Adds suffix to ID fields to ensure uniqueness
  * Adds LA name
  * Adds detected year

Tables and columns names can be provided through configuration. Other functions can be added to the enrichment pipeline as required.

Inputs:
  * Dataframes for each table
  * Metadata

Outputs:
  * Dataframes for each table

## Apply Privacy Policy

Removes sensitive columns and data, or masks / blanks / degrades the data to meet data minimisation rules.

Working on each of the tables in turn, this process will degrade the data to meet data minimisation rules:
  * Dates all set to the first of the month
  * Postcodes all set to the first 4 characters (excluding spaces)
  * Some tables need rows deleted if there are blanks in a specific column

Inputs:
  * Dataframes for each table

Outputs:
  * Dataframes for retained tables

## History 1 - Archive data
## History 2 - Rollup data
## Client reports
## Prepare shareable reports
## Data Retention 1 - Clear old history data
## Data Retention 2 - Clear old session data
## Data Retention 3 - Clear incoming data
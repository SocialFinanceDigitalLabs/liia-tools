# ===== Main script where all the others will be called =====

import time
import SWFtools.util.AppLogs as AppLog
import SWFtools.util.work_path as work_path
import SWFtools.dataprocessing.file_operations as fop
from SWFtools.analysis.growth_tables import growth_tables
from SWFtools.analysis.pivotGen import pivotGen
from SWFtools.analysis.seniority import seniority
from SWFtools.analysis.progressed import progressed
from SWFtools.analysis.seniority_forecast_5c import seniority_forecast_5c
from SWFtools.analysis.seniority_forecast_04 import seniority_forecast_04
from SWFtools.analysis.FTESum import FTESum
from SWFtools.analysis.FTESum_2020 import FTESum_2020

start = time.time()

# Initialising logger (present throughout the program)
AppLog.initialise()

# CONVERSION STEPS
#Recreate all folders present in 'cin' into 'flatfiles' adding a 'la_log' folder to store all runtime logs
work_path.check_flatfiles_folder()

# Validate, process, and convert all XML files found in LA directories to CSV then merge them
fop.process_all_input_files()

""" For creating spreadsheets, tables and other files that will be stored in the request folder """


# Outputs a file of extension 'xlsx' named 'growth_tables' (does not process any file, contains hardcoded values)
growth_tables()
# Creates a pivot table grouping data on Census year, age, gender, and LEA name; and summing on FTE and SWE
pivotGen()

seniority()
progressed()
seniority_forecast_5c()
FTESum()
FTESum_2020()
seniority_forecast_04()

end = time.time()
total_time = round(end-start, 3)

print()
print("Execution Time:", total_time)

AppLog.log_footer(total_time)

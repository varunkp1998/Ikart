# """ script for convrting data to json"""
import logging
import os

# task_logger = logging.getLogger('task_logger')

# def write(json_data: dict, datafram, counter) -> bool:
#     """ function for converting csv  to  json"""
#     try:
#         task_logger.info("converting data to json initiated")
#         target = json_data["task"]["target"]
#         if counter == 1:
#             if os.path.exists(target["file_path"]+target["file_name"]):
#                 os.remove(target["file_path"]+target["file_name"])
#                 datafram.to_json(target["file_path"] + \
#                 target["file_name"],orient='records',
#                 lines = True, index=True, mode = 'a')
#                 task_logger.info("json conversion completed")
#         else:
#             datafram.to_json(target["file_path"] + \
#             target["file_name"],orient='records',
#                 lines = True, index=True, mode = 'a')
#         return True
#     except Exception as error:
#         task_logger.exception("convert_csv_to_json() is %s", str(error))
#         raise error

# import logging
# import os
# import json
# import numpy as np
# import math
# import pandas as pd

# task_logger = logging.getLogger('task_logger')

# def write(json_data: dict, datafram, counter) -> bool:
#     """Function for converting csv to json"""
#     try:
#         task_logger.info("Converting data to JSON initiated")
#         target = json_data["task"]["target"]

#         # Replace 'NaN' strings with None in the DataFrame
#         # datafram = datafram.applymap(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.lower() == 'nan') else x)
#         datafram.replace({np.nan: None}, inplace=True)
#         # datafram = datafram.applymap(lambda x: None if pd.isna(x) or (isinstance(x, str) and x == 'NaN') else x)

#         json_list = datafram.to_dict(orient='records')

#         if counter == 1:
#             # Write the entire list of dictionaries to a new JSON file
#             with open(target["file_path"] + target["file_name"], 'w') as json_file:
#                 json.dump(json_list, json_file, indent=4)
#         else:
#             # Append the list of dictionaries to the existing JSON file
#             with open(target["file_path"] + target["file_name"], 'r+') as json_file:
#                 json_file.seek(0, os.SEEK_END)  # Move to the end of the file
#                 pos = json_file.tell()  # Get the current position
#                 if pos > 1:  # If the file is not empty, remove the last character (']')
#                     json_file.seek(pos - 1)
#                     json_file.truncate()
#                     json_file.write(',')  # Add a comma before appending the new chunk
#                 json.dump(json_list, json_file, indent=4)
#                 json_file.write(']')  # Close the JSON array
        
#         task_logger.info("JSON conversion completed")
#         return True
#     except Exception as error:
#         task_logger.exception("convert_csv_to_json() error: %s", str(error))
#         raise error

import logging
import os
import json
import numpy as np
import math
import pandas as pd

task_logger = logging.getLogger('task_logger')

def write(json_data: dict, datafram, counter) -> bool:
    """Function for converting csv to json"""
    try:
        task_logger.info("Converting data to JSON initiated")
        target = json_data["task"]["target"]

        # # Replace 'NaN' strings with None in the DataFrame
        # datafram.replace({np.nan: None}, inplace=True)

        # json_list = datafram.to_dict(orient='records')

        json_filename = target["file_path"] + target["file_name"]

        if counter == 1:
            # Create a new DataFrame to store merged data
            merged_dataframe = datafram.copy()
        else:
            # Append data to the existing merged DataFrame
            merged_dataframe = merged_dataframe.append(datafram, ignore_index=True)
        
        with open(json_filename, 'w') as json_file:
            json_file.write('[')  # Open the JSON array
            json.dump(merged_dataframe.to_dict(orient='records'), json_file, indent=4)
            json_file.write(']')  # Close the JSON array
        
        task_logger.info("JSON conversion completed")
        return True
    except Exception as error:
        task_logger.exception("convert_csv_to_json() error: %s", str(error))
        raise error

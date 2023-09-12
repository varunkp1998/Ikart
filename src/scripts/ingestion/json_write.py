# """ script for convrting data to json"""
import logging
import os
from datetime import datetime
import pandas as pd
from utility import replace_date_placeholders

task_logger = logging.getLogger('task_logger')

def write(json_data: dict, dataframe, counter) -> bool:
    """ function for writing to JSON """
    try:
        target = json_data["task"]["target"]
        file_path = target["file_path"]
        file_name = target["file_name"]
        file_name = replace_date_placeholders(target['file_name'])
        task_logger.info("converting data to JSON initiated")
        check = True if target['index'] == "False" else False
        # Reset the index and create an 'index' column
        dataframe.reset_index(drop=check, inplace=True)

        if counter == 1: # If it's the first chunk, write the data to a new JSON file
            if os.path.exists(target["file_path"]+file_name):
                    os.remove(target["file_path"]+file_name)
            if target["audit_columns"] == "active":
                # if audit_columns are active
                dataframe['CRTD_BY']="etl_user"
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                dataframe.to_json(file_path + file_name, orient='records', lines=False)
            else:
                dataframe.to_json(file_path + file_name, orient='records', lines=False)
        else: # If it's not the first chunk, read the existing JSON file and append the new data
            if target["audit_columns"] == "active":
                # if audit_columns are active
                dataframe['CRTD_BY']="etl_user"
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                existing_dataframe = pd.read_json(file_path + file_name, orient='records')
                updated_dataframe = pd.concat([existing_dataframe, dataframe], ignore_index=True)
                updated_dataframe.to_json(file_path + file_name, orient='records', lines=False)
            else:
                existing_dataframe = pd.read_json(file_path + file_name, orient='records')
                updated_dataframe = pd.concat([existing_dataframe, dataframe], ignore_index=True)
                updated_dataframe.to_json(file_path + file_name, orient='records', lines=False)
        task_logger.info("JSON conversion completed")
        return True
    except Exception as error:
        task_logger.exception("converting_to_json() is %s", str(error))
        raise error

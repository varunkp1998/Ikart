""" script for convrting data to excel"""
import logging
import os
from datetime import datetime
import pandas as pd
from utility import replace_date_placeholders

task_logger = logging.getLogger('task_logger')

def write(json_data: dict, dataframe, counter) -> bool:
    """ function for writing to Excel """
    try:
        target = json_data["task"]["target"]
        file_path = target["file_path"]
        file_name = target["file_name"]
        file_name = replace_date_placeholders(target['file_name'])
        task_logger.info("converting data to Excel initiated")
        check = True if target['index'] == "False" else False
        # Reset the index and create an 'index' column
        dataframe.reset_index(drop=check, inplace=True)

        if counter == 1: # If it's the first chunk, write the data to a new Excel file
            if os.path.exists(target["file_path"]+file_name):
                os.remove(target["file_path"]+file_name)
            if target["audit_columns"] == "active":
                # if audit_columns are active
                dataframe['CRTD_BY']="etl_user"
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                dataframe.to_excel(file_path + file_name, index=False)
            else:
                dataframe.to_excel(file_path + file_name, index=False)
        else: # If it's not the first chunk, read the existing Excel file and append the new data
            if target["audit_columns"] == "active":
                # if audit_columns are active
                dataframe['CRTD_BY']="etl_user"
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                existing_dataframe = pd.read_excel(file_path + file_name)
                updated_dataframe = pd.concat([existing_dataframe, dataframe], ignore_index=True)
                updated_dataframe.to_excel(file_path + file_name, index=False)
            else:
                existing_dataframe = pd.read_excel(file_path + file_name)
                updated_dataframe = pd.concat([existing_dataframe, dataframe], ignore_index=True)
                updated_dataframe.to_excel(file_path + file_name, index=False)
        task_logger.info("Excel conversion completed")
        return True
    except Exception as error:
        task_logger.exception("converting_to_excel() is %s", str(error))
        raise error




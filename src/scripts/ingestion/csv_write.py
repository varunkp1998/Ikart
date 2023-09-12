""" script for writing data to csv file"""
import logging
from datetime import datetime
import os
from ast import literal_eval
from utility import replace_date_placeholders

task_logger = logging.getLogger('task_logger')

def write(json_data: dict,datafram, counter) -> bool:
    """ function for writing data to csv file"""
    try:
        target = json_data["task"]["target"]
        file_name = replace_date_placeholders(target['file_name'])
        task_logger.info("writing data to csv file")
        if counter ==1: # for first iteration
            if target["audit_columns"] == "active":
                # if audit_columns are active
                datafram['CRTD_BY']="etl_user"
                datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datafram['UPDT_BY']= " "
                datafram['UPDT_DTTM']= " "
                if os.path.exists(target["file_path"]+file_name):
                    os.remove(target["file_path"]+file_name)
                datafram.to_csv(target["file_path"]+file_name,
                sep=target["delimiter"], #header=literal_eval(target["header"]),
                index=literal_eval(target["index"]), mode='a',
                encoding=target["encoding"])
            else:
                # if audit_columns are  not active
                if os.path.exists(target["file_path"]+file_name):
                    os.remove(target["file_path"]+file_name)
                datafram.to_csv(target["file_path"]+file_name,
                sep=target["delimiter"], #header=literal_eval(
                # target["header"]),
                index=literal_eval(target["index"]), mode='a',
                encoding=target["encoding"])
        else: # for iterations other than one
            if target["audit_columns"] == "active":
                # if audit_columns are active
                datafram['CRTD_BY']="etl_user"
                datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datafram['UPDT_BY']= " "
                datafram['UPDT_DTTM']= " "
                datafram.to_csv(target["file_path"]+file_name,
                sep=target["delimiter"], header=False,
                index=literal_eval(target["index"]),
                mode='a', encoding=target["encoding"])
            else:
                # if audit_columns are  not active
                datafram.to_csv(target["file_path"]+file_name,
                sep=target["delimiter"], header=False,
                index=literal_eval(target["index"]),
                mode='a', encoding=target["encoding"])
        return True
    except Exception as error:
        task_logger.exception("ingest_data_to_csv() is %s", str(error))
        raise error

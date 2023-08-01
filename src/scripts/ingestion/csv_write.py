""" script for writing data to csv file"""
import logging
from datetime import datetime
import os
from ast import literal_eval

task_logger = logging.getLogger('task_logger')
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def write_datafram_to_csv(target,datafram):
    '''function to write the data from api to csv file'''
    if target["audit_columns"] == "active":
        datafram['CRTD_BY']="etl_user"
        datafram['CRTD_DTTM']= datetime.now().strftime(DATE_FORMAT)
        datafram['UPDT_BY']= " "
        datafram['UPDT_DTTM']= " "
        if os.path.exists(target["file_path"]+target["file_name"]):
            os.remove(target["file_path"]+target["file_name"])
        datafram.to_csv(target["file_path"]+target["file_name"],
        sep=target["delimiter"], header=literal_eval(
        target["header"]),
        index=literal_eval(target["index"]), mode='a',
        encoding=target["encoding"])
    else:
        if os.path.exists(target["file_path"]+target["file_name"]):
            os.remove(target["file_path"]+target["file_name"])
        datafram.to_csv(target["file_path"]+target["file_name"],
        sep=target["delimiter"], header=literal_eval(
        target["header"]),
        index=literal_eval(target["index"]), mode='a',
        encoding=target["encoding"])

def other_than_rest_api_read(target,datafram,counter):
    '''function for writing the data to csv for all different sources
    other than rest_api'''
    if counter ==1: # for first iteration
        write_datafram_to_csv(target,datafram)
    else: # for iterations other than one
        if target["audit_columns"] == "active":
            # if audit_columns are active
            datafram['CRTD_BY']="etl_user"
            datafram['CRTD_DTTM']= datetime.now().strftime(DATE_FORMAT)
            datafram['UPDT_BY']= " "
            datafram['UPDT_DTTM']= " "
            datafram.to_csv(target["file_path"]+target["file_name"],
            sep=target["delimiter"], header=False,
            index=literal_eval(target["index"]),
            mode='a', encoding=target["encoding"])
        else: # if audit_columns are  not active
            datafram.to_csv(target["file_path"]+target["file_name"],
            sep=target["delimiter"], header=False,
            index=literal_eval(target["index"]),
            mode='a', encoding=target["encoding"])

def write(json_data: dict,datafram, counter) -> bool:
    """ function for writing data to csv file"""
    try:
        target = json_data["task"]["target"]
        task_logger.info("writing data to csv file")
        if json_data['task']['source']['source_type'] == "rest_api_read":
            write_datafram_to_csv(target,datafram)
        else:
            other_than_rest_api_read(target,datafram,counter)
        # audit(json_data, task_id,run_id,'TGT_RECORD_COUNT',datafram.shape[0],
        #         iter_value)
        return True
    except Exception as error:
        task_logger.exception("ingest_data_to_csv() is %s", str(error))
        raise error

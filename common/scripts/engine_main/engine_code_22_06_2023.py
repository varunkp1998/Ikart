""" importing modules """
import json
import logging
import sys
import os
from datetime import datetime
import importlib
import requests
import pandas as pd

log2 = logging.getLogger('log2')
FAIL_LOG_STATEMENT = "%s got failed engine"
TASK_LOG = 'Task %s Execution Completed'

def write_to_txt1(task_id,status,file_path):
    """Generates a text file with statuses for orchestration"""
    try:
        # # Acquire the lock before reading from the file
        # lock.acquire()
        is_exist = os.path.exists(file_path)
        if is_exist is True:
            data_fram =  pd.read_csv(file_path, sep='\t')
            data_fram.loc[data_fram['task_name']==task_id, 'Job_Status'] = status
            data_fram.to_csv(file_path ,mode='w', sep='\t',index = False, header=True)
        else:
            log2.error("pipeline txt file does not exist")
    except pd.errors.EmptyDataError as error:
        log2.error("The file is empty or has no columns to parse.")
        raise error
    except Exception as error:
        log2.exception("write_to_txt: %s.", str(error))
        raise error

def audit(json_data, task_name,run_id,status,value,itervalue):
    """ create audit json file and audits event records into it"""
    try:
        url = "http://localhost:8080/api/audit"
        audit_data = [{
                    "pipeline_id": json_data["pipeline_id"],
                    "task/pipeline_name": task_name,
                    "run_id": run_id,
                    "iteration": itervalue,
                    "audit_type": status,
                    "audit_value": value,
                    "process_dttm" : datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                }]
        response = requests.post(url, json=audit_data, timeout=60)
        log2.info("audit status code:%s",response.status_code)
    except Exception as error:
        log2.exception("error in audit %s.", str(error))
        raise error

def task_json_read(paths_data,task_id,prj_nm):
    """function to read task json"""
    try:
        with open(r""+paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["task_json_path"]+task_id+".json","r",encoding='utf-8') as jsonfile:
            log2.info("reading TASK JSON data started %s",task_id)
            json_data = json.load(jsonfile)
            log2.info("reading TASK JSON data completed")
        return json_data
    except Exception as error:
        log2.exception("error in task_json_read %s.", str(error))
        raise error

def checks_mapping_read(paths_data):
    """function to read task json"""
    try:
        with open(r""+paths_data["folder_path"]+paths_data["dq_scripts_path"]+\
        "checks_mapping.json","r",encoding='utf-8') as json_data_new:
            log2.info("reading checks mapping json data started")
            json_checks = json.load(json_data_new)
            log2.info("reading checks mapping data completed")
        return json_checks
    except Exception as error:
        log2.exception("error in checks_mapping_read %s.", str(error))
        raise error

def read_write_imports(paths_data,json_data):
    """function for importing read and write functions"""
    try:
        py_scripts_path=paths_data["folder_path"]+paths_data["ingestion_path"]
        #log2.info(py_scripts_path)
        sys.path.insert(0, py_scripts_path)
        log2.info("read imports started")
        module_name = json_data["task"]["source"]["source_type"]
        log2.info("write imports started")
        module_name1 = json_data["task"]["target"]["target_type"]
        module = importlib.import_module(module_name)
        module1 = importlib.import_module(module_name1)
        read = getattr(module, "read")
        write = getattr(module1, "write")
        return read,write
    except Exception as error:
        log2.exception("error in read_write_imports %s.", str(error))
        raise error

def task_failed(task_id,file_path,json_data,run_id,iter_value):
    """function to log and audit if task is failed"""
    try:
        write_to_txt1(task_id,'FAILED',file_path)
        audit(json_data,task_id,run_id,'STATUS','FAILED',iter_value)
        log2.warning(FAIL_LOG_STATEMENT, task_id)
    except Exception as error:
        log2.exception("error in task_failed %s.", str(error))
        raise error

def task_success(task_id,file_path,json_data,run_id,iter_value):
    """function to log and audit if task is success"""
    try:
        write_to_txt1(task_id,'SUCCESS',file_path)
        audit(json_data,task_id,run_id,'STATUS','COMPLETED',
                iter_value)
        log2.info(TASK_LOG,task_id)
    except Exception as error:
        log2.exception("error in task_success %s.", str(error))
        raise error

def data_quality_features(json_data,definitions_qc):
    """function for executing data quality features based on pre and post checks"""
    try:
        if json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
        and json_data['task']['data_quality_features']['data_masking_required'] == 'Y':
            definitions_qc.auto_correction(json_data)
            definitions_qc.data_masking(json_data)
        elif json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
        and json_data['task']['data_quality_features']['data_encryption_required'] == 'Y':
            definitions_qc.auto_correction(json_data)
            definitions_qc.data_encryption(json_data)
        elif json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
        and json_data['task']['data_quality_features']['data_masking_required'] == 'Y' \
        and json_data['task']['data_quality_features']['data_encryption_required'] == 'Y':
            definitions_qc.auto_correction(json_data)
            definitions_qc.data_masking(json_data)
            definitions_qc.data_encryption(json_data)
        elif json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y':
            definitions_qc.auto_correction(json_data)
        elif json_data['task']['data_quality_features']['data_masking_required'] == 'Y':
            definitions_qc.data_masking(json_data)
        elif json_data['task']['data_quality_features']['data_encryption_required'] == 'Y':
            definitions_qc.data_encryption(json_data)
    except Exception as error:
        log2.exception("error in data_quality_features %s.", str(error))
        raise error

def engine_main(prj_nm,task_id,paths_data,run_id,file_path,iter_value):
    """function consists of pre_checks,conversion,ingestion,post_checks, qc report"""
    try:
        log2.info("entered into engine_main")
        json_data = task_json_read(paths_data,task_id,prj_nm)
        write_to_txt1(task_id,'STARTED',file_path)
        audit(json_data, task_id,run_id,'STATUS','STARTED',iter_value)
        json_checks = checks_mapping_read(paths_data)
        config_file_path = paths_data["folder_path"]+paths_data["config_path"]
        dq_scripts_path=paths_data["folder_path"]+paths_data["dq_scripts_path"]
        sys.path.insert(0, dq_scripts_path)
        definitions_qc = importlib.import_module("definitions_qc")

        # Precheck script execution starts here
        if json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'Y' and\
        json_data["task"]["source"]["source_type"] in ('csv_read','postgres_read','mysql_read',
        'snowflake_read','mssql_read'):
            pre_check = definitions_qc.qc_pre_check(prj_nm,json_data,json_checks,
            paths_data,config_file_path,task_id,run_id,file_path,iter_value)
        elif json_data["task"]["source"]["source_type"] == "csv_read" and \
        (json_data['task']['data_quality_execution']['pre_check_enable'] == 'N' and \
        json_data['task']['data_quality_execution']['post_check_enable'] == 'N'):
            data_quality_features(json_data,definitions_qc)

        # ingestion execution starts here
        read, write =read_write_imports(paths_data,json_data)
        if json_data["task"]["source"]["source_type"] in ("postgres_read","mysql_read",
        "snowflake_read","sqlserver_read"):
            data_fram = read(json_data,config_file_path,task_id,run_id,paths_data,
            file_path,iter_value)
            counter=0
            for i in data_fram :
                counter+=1
                if json_data["task"]["target"]["target_type"] != "csv_write":
                    value=write(json_data, i,counter,config_file_path,task_id,run_id,
                    paths_data,file_path,iter_value)
                    if value is False:
                        task_failed(task_id,file_path,json_data,run_id,iter_value)
                        return False
                    if value is True:
                        task_success(task_id,file_path,json_data,run_id,iter_value)
                        return False
                else:
                    value=write(json_data, i,counter)
                    if value is False:
                        task_failed(task_id,file_path,json_data,run_id,iter_value)
                        return False
        elif json_data["task"]["source"]["source_type"] == "csv_read":
            data_fram=read(json_data,task_id,run_id,paths_data,file_path,iter_value)
            counter=0
            for i in data_fram :
                counter+=1
                if json_data["task"]["target"]["target_type"] != "csv_write":
                    value=write(json_data, i,counter,config_file_path,task_id,run_id,
                    paths_data,file_path,iter_value)
                    if value is False:
                        task_failed(task_id,file_path,json_data,run_id,iter_value)
                        return False
                    if value is True:
                        task_success(task_id,file_path,json_data,run_id,iter_value)
                        return False
                else:
                    value=write(json_data, i,counter)
                    if value is False:
                        task_failed(task_id,file_path,json_data,run_id,iter_value)
                        return False
        elif json_data["task"]["source"]["source_type"] in ("csvfile_read"
            ,"json_read" ,"xml_read","parquet_read","excel_read"):
            data_fram=read(json_data,task_id,run_id,paths_data,file_path,iter_value)
            counter=0
            for i in data_fram :
                # log2.info(i)
                counter+=1
                if json_data["task"]["target"]["target_type"] == "csvfile_write":
                    value=write(json_data, i,counter)
                    if value=='Fail':
                        task_failed(task_id,file_path,json_data,run_id,iter_value)
                        return False
                elif json_data["task"]["target"]["target_type"] == "s3_write":
                    value=write(json_data, i,config_file_path,task_id,run_id,
                    paths_data,file_path,iter_value)
                    if value=='Fail':
                        task_failed(task_id,file_path,json_data,run_id,iter_value)
                        return False
                else:
                    value=write(json_data, i)
        else:
            log2.info("only ingestion available currently")

        # postcheck script execution starts here
        if json_data["task"]["target"]["target_type"] in ('csv_write'
        ,'postgres_write' ,'mysql_write',"snowflake_write",'mssql_write' ) and \
        json_data["task"]["data_quality_execution"]["post_check_enable"] == 'Y':
            # post check code
            post_check=definitions_qc.qc_post_check(prj_nm,json_data, json_checks,paths_data,
            config_file_path,task_id,run_id,file_path,iter_value)
        #qc report generation
        new_path=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["qc_reports_path"]
        if json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'Y' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'N':
            post_check = pd.DataFrame()
            definitions_qc.qc_report(pre_check,post_check,new_path,file_path,
                         json_data,task_id,run_id,iter_value)
        elif json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'N' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'Y':
            pre_check = pd.DataFrame()
            definitions_qc.qc_report(pre_check,post_check,new_path,file_path,
                         json_data,task_id,run_id,iter_value)
        elif json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'Y' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'Y':
            definitions_qc.qc_report(pre_check,post_check,new_path,file_path,
                         json_data,task_id,run_id,iter_value)
        log2.info(TASK_LOG,task_id)
        write_to_txt1(task_id,'SUCCESS',file_path)
        audit(json_data, task_id,run_id,'STATUS','COMPLETED',iter_value)
    except Exception as error:
        audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
        write_to_txt1(task_id,'FAILED',file_path)
        log2.warning(FAIL_LOG_STATEMENT, task_id)
        log2.exception("error in  %s.", str(error))
        raise error

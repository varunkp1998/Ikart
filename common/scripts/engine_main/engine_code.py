""" importing modules """
import json
import logging
import sys
import os
from datetime import datetime
from os import path
import pandas as pd

log2 = logging.getLogger('log2')

def write_to_txt1(prj_nm,task_id,status,run_id,paths_data):
    """Generates a text file with statuses for orchestration"""
    try:
        place=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["status_txt_file_path"]+run_id+".txt"
        is_exist = os.path.exists(place )
        if is_exist is True:
            # log2.info("txt getting called")
            data_fram =  pd.read_csv(place, sep='\t')
            data_fram.loc[data_fram['task_name']==task_id, 'Job_Status'] = status
            data_fram.to_csv(place ,mode='w', sep='\t',index = False, header=True)
    except Exception as error:
        log2.exception("write_to_txt1: %s.", str(error))
        raise error

def audit(json_file_path,json_data, task_name,run_id,status,value):
    """ create audit json"""
    try:
        if path.isfile(json_file_path) is False:
            log2.info('audit started')
            # Data to be written
            audit_data = [{
                "project_id": json_data["project_id"],
                "task/pipeline_name": task_name,
                "run_id": run_id,
                "iteration": "1",
                "audit_type": status,
                "audit_value": value,
                "process_dttm" : datetime.now()
            }]
            # Serializing json
            json_object = json.dumps(audit_data, indent=4, default=str)
            # Writing to sample.json
            with open(json_file_path, "w", encoding='utf-8') as outfile:
                outfile.write(json_object)
            # outfile.close()
            log2.info("audit json file created and audit done")
        else:
            with open(json_file_path, "r+", encoding='utf-8') as audit1:
                audit_data = json.load(audit1)
                audit_data.append(
                    {
                    "project_id": json_data["project_id"],
                    "task/pipeline_name": task_name,
                    "run_id": run_id,
                    "iteration": "1",
                    "audit_type": status,
                    "audit_value": value,
                    "process_dttm" : datetime.now()
                    })
                audit1.seek(0)
                json.dump(audit_data, audit1, indent=4, default=str)
                # audit1.close()
                log2.info('Audit of an event has been made')
    except Exception as error:
        log2.exception("error in auditing json %s.", str(error))
        raise error


def engine_main(prj_nm,task_id,paths_data,run_id,pip_nm):
    """function consists of pre_checks,conversion,ingestion,post_checks, qc report"""
    log2.info("entered into engine_main")
    config_file_path = paths_data["folder_path"]+paths_data["config_path"]
    dq_scripts_path=paths_data["folder_path"]+paths_data["dq_scripts_path"]
    sys.path.insert(0, dq_scripts_path)
    import definitions_qc as dq
    audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
    paths_data["audit_path"]+task_id+'_audit_'+run_id+'.json'
    try:
        with open(r""+paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["task_json_path"]+task_id+".json","r",encoding='utf-8') as jsonfile:
            log2.info("reading TASK JSON data started %s",task_id)
            json_data = json.load(jsonfile)
            log2.info("reading TASK JSON data completed")
            write_to_txt1(prj_nm,task_id,'STARTED',run_id,paths_data)
            audit(audit_json_path,json_data, task_id,run_id,'STATUS','STARTED')
    except Exception as error:
        log2.exception("error in reading json %s.", str(error))
        raise error
    try:
        with open(r""+paths_data["folder_path"]+paths_data["dq_scripts_path"]+\
        "checks_mapping.json","r",encoding='utf-8') as json_data_new:
            log2.info("reading checks mapping json data started")
            json_checks = json.load(json_data_new)
            log2.info("reading checks mapping data completed")

        dq_scripts_path=paths_data["folder_path"]+paths_data["dq_scripts_path"]
        sys.path.insert(0, dq_scripts_path)
        from definitions_qc import auto_correction,data_masking,data_encryption

        # Precheck script execution starts here
        if json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'Y' and\
        (json_data["task"]["source"]["source_type"] == 'csv_read' or\
        json_data["task"]["source"]["source_type"] == 'postgres_read' or\
        json_data["task"]["source"]["source_type"] == 'mysql_read' or\
	    json_data["task"]["source"]["source_type"] == 'snowflake_read'or\
        json_data["task"]["source"]["source_type"] == 'mssql_read'):
            pre_check = dq.qc_pre_check(prj_nm,json_data,json_checks,paths_data,config_file_path,
            task_id,run_id)
        elif json_data["task"]["source"]["source_type"] == "csv_read" and \
        (json_data['task']['data_quality_execution']['pre_check_enable'] == 'N' and \
        json_data['task']['data_quality_execution']['post_check_enable'] == 'N'):
            if json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
            and json_data['task']['data_quality_features']['data_masking_required'] == 'Y':
                auto_correction(json_data)
                data_masking(json_data)
            elif json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
            and json_data['task']['data_quality_features']['data_encryption_required'] == 'Y':
                auto_correction(json_data)
                data_encryption(json_data)
            elif json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
            and json_data['task']['data_quality_features']['data_masking_required'] == 'Y' \
            and json_data['task']['data_quality_features']['data_encryption_required'] == 'Y':
                auto_correction(json_data)
                data_masking(json_data)
                data_encryption(json_data)
            elif json_data['task']['data_quality_features']['dq_auto_correction_required'] == 'Y':
                auto_correction(json_data)
            elif json_data['task']['data_quality_features']['data_masking_required'] == 'Y':
                data_masking(json_data)
            elif json_data['task']['data_quality_features']['data_encryption_required'] == 'Y':
                data_encryption(json_data)

        py_scripts_path=paths_data["folder_path"]+paths_data["ingestion_path"]
        #log2.info(py_scripts_path)
        sys.path.insert(0, py_scripts_path)
        #file conversion code and ingestion code
        log2.info("read imports started")
        #file conversion code and ingestion code
        if json_data["task"]["source"]["source_type"] == "csv_read":
            from csv_read import read
        elif json_data["task"]["source"]["source_type"] == "postgres_read":
            from postgres_read import read
        elif json_data["task"]["source"]["source_type"] == "snowflake_read":
            from snowflake_read import read
        elif json_data["task"]["source"]["source_type"] == "mysql_read":
            from mysql_read import read
        elif json_data["task"]["source"]["source_type"] == "csvfile_read":
            from csvfile_read import read
        elif json_data["task"]["source"]["source_type"] == "parquetfile_read":
            from parquet_read import read
        elif json_data["task"]["source"]["source_type"] == "excelfile_read":
            from excel_read import read
        elif json_data["task"]["source"]["source_type"] == "jsonfile_read":
            from json_read import read
        elif json_data["task"]["source"]["source_type"] == "xmlfile_read":
            from xml_read import read
        elif json_data["task"]["source"]["source_type"] == "textfile_read":
            from text_read import read
        elif json_data["task"]["source"]["source_type"] == "sqlserver_read":
            from sqlserver_read import read


        if json_data["task"]["target"]["target_type"] == "postgres_write":
            from postgres_write import write
        elif json_data["task"]["target"]["target_type"] == "mysql_write":
            from mysql_write import write
        elif json_data["task"]["target"]["target_type"] == "snowflake_write":
            from snowflake_write import write
        elif json_data["task"]["target"]["target_type"] == "parquetfile_write":
            from parquet_write import write
        elif json_data["task"]["target"]["target_type"] == "csv_write":
            from csv_write import write
        elif json_data["task"]["target"]["target_type"] == "csvfile_write":
            from csvfile_write import write
        elif json_data["task"]["target"]["target_type"] == "excelfile_write":
            from excel_write import write
        elif json_data["task"]["target"]["target_type"] == "jsonfile_write":
            from json_write import write
        elif json_data["task"]["target"]["target_type"] == "xmlfile_write":
            from xml_write import write
        elif json_data["task"]["target"]["target_type"] == "textfile_write":
            from text_write import write
        elif json_data["task"]["target"]["target_type"] == "sqlserver_write":
            from sqlserver_write import write


        # main script execution starts here
        if json_data["task"]["source"]["source_type"] == "postgres_read" or \
            json_data["task"]["source"]["source_type"] == "mysql_read" or\
	        json_data["task"]["source"]["source_type"] == "snowflake_read" or\
            json_data["task"]["source"]["source_type"] == "sqlserver_read":
            data_fram=read(prj_nm,json_data,config_file_path,task_id,run_id,paths_data, pip_nm)
            # log2.info(data_fram.__next__())
            counter=0
            for i in data_fram :
                # log2.info(i)
                counter+=1
                if json_data["task"]["target"]["target_type"] != "csv_write":
                    value=write(prj_nm,json_data, i,counter,config_file_path,task_id,run_id,
                    paths_data, pip_nm)
                    if value=='Fail':
                        write_to_txt1(prj_nm,task_id,'FAILED',run_id,paths_data)
                        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
                        log2.warning("%s  got failed engine", task_id)
                        return False
                    if value=='drop_Pass':
                        write_to_txt1(prj_nm,task_id,'SUCCESS',run_id,paths_data)
                        audit(audit_json_path,json_data, task_id,run_id,'STATUS','COMPLETED')
                        log2.info('Task %s Execution Completed',task_id)
                        return False
                else:
                    value=write(json_data, i,counter)
                    if value=='Fail':
                        write_to_txt1(prj_nm,task_id,'FAILED',run_id,paths_data)
                        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
                        log2.warning("%s  got failed engine", task_id)
                        return False
        elif json_data["task"]["source"]["source_type"] == "csv_read":
            data_fram=read(prj_nm,json_data,task_id,run_id,pip_nm,paths_data)
                         # log2.info(data_fram.__next__())
            counter=0
            for i in data_fram :
                # log2.info(i)
                counter+=1
                if json_data["task"]["target"]["target_type"] != "csv_write":
                    value=write(prj_nm,json_data, i,counter,config_file_path,task_id,run_id,
                    paths_data, pip_nm)
                    if value=='Fail':
                        write_to_txt1(prj_nm,task_id,'FAILED',run_id,paths_data)
                        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
                        log2.warning("%s  got failed engine", task_id)
                        return False
                else:
                    value=write(json_data, i,counter)
                    if value=='Fail':
                        write_to_txt1(prj_nm,task_id,'FAILED',run_id,paths_data)
                        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
                        log2.warning("%s  got failed engine", task_id)
                        return False
            #log2.info(data_fram.__next__())
        elif json_data["task"]["source"]["source_type"] == "csvfile_read" or \
            json_data["task"]["source"]["source_type"] == "jsonfile_read" or \
            json_data["task"]["source"]["source_type"] == "xmlfile_read" or \
            json_data["task"]["source"]["source_type"] == "parquetfile_read" or \
            json_data["task"]["source"]["source_type"] == "excelfile_read":
            data_fram=read(prj_nm,json_data,task_id,run_id,pip_nm,paths_data)
            # log2.info(data_fram.__next__())
            counter=0
            for i in data_fram :
                # log2.info(i)
                counter+=1
                # value=write(json_data, i)
                if json_data["task"]["target"]["target_type"] == "csvfile_write":
                    value=write(json_data, i,counter)
                    if value=='Fail':
                        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
                        write_to_txt1(prj_nm,task_id,'FAILED',run_id,paths_data)
                        log2.warning("%s  got failed engine", task_id)
                        return False
                else:
                    # log2.info("entered")
                    value=write(json_data, i)
        else:
            log2.info("only ingestion available currently")

        # postcheck script execution starts here
        if (json_data["task"]["target"]["target_type"] == 'csv_write' or\
        json_data["task"]["target"]["target_type"] == 'postgres_write' or \
        json_data["task"]["target"]["target_type"] == 'mysql_write' or\
        json_data["task"]["target"]["target_type"] == "snowflake_write" or\
        json_data["task"]["target"]["target_type"] == 'mssql_write' ) and \
        json_data["task"]["data_quality_execution"]["post_check_enable"] == 'Y':
            # post check code
            post_check=dq.qc_post_check(prj_nm,json_data, json_checks,paths_data,
            config_file_path,task_id,run_id)
            #qc report generation
        new_path=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["qc_reports_path"]
        if json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'N' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'N':
            pass
        elif json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'Y' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'N':
            post_check = pd.DataFrame()
            dq.qc_report(pre_check, post_check,new_path)
        elif json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'N' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'Y':
            pre_check = pd.DataFrame()
            dq.qc_report(pre_check, post_check,new_path)
        elif json_data["task"]["data_quality_execution"]["pre_check_enable"] == 'Y' and \
            json_data["task"]["data_quality_execution"]["post_check_enable"] == 'Y':
            dq.qc_report(pre_check, post_check,new_path)
        # log2.info(qc_report)
        log2.info('Task %s Execution Completed',task_id)
        # write_to_txt(Task_id,Status1)
        write_to_txt1(prj_nm,task_id,'SUCCESS',run_id,paths_data)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','COMPLETED')
    except Exception as error:
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        write_to_txt1(prj_nm,task_id,'FAILED',run_id,paths_data)
        # write_to_txt(Task_id,Status1)
        log2.warning("%s got failed engine", task_id)
        log2.exception("error in  %s.", str(error))
        raise error

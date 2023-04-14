""" script for writing data to S3"""
import logging
from io import StringIO
import os
import sys
import pandas as pd
import boto3
from utility import get_config_section,decrypt

log2 = logging.getLogger('log2')

def establish_conn(json_data: dict, json_section: str,config_file_path:str):
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(config_file_path+json_data["task"][json_section]\
        ["connection_name"]+'.json')
        d_aws_access_key_id = decrypt(connection_details["aws_access_key_id"])
        d_aws_secret_access_key = decrypt(connection_details["aws_secret_access_key"])
        conn = boto3.client( service_name= 's3',region_name=
        connection_details["region_name"],aws_access_key_id=d_aws_access_key_id,
        aws_secret_access_key=d_aws_secret_access_key)
        logging.info("connection established")
        return conn
    except Exception as error:
        log2.exception("establish_conn() is %s", str(error))
        raise error

def write_to_txt(task_id,status,file_path):
    """Generates a text file with statuses for orchestration"""
    try:
        is_exist = os.path.exists(file_path)
        if is_exist is True:
            # log2.info("txt getting called")
            data_fram =  pd.read_csv(file_path, sep='\t')
            data_fram.loc[data_fram['task_name']==task_id, 'Job_Status'] = status
            data_fram.to_csv(file_path ,mode='w', sep='\t',index = False, header=True)
        else:
            log2.error("pipeline txt file does not exist")
    except Exception as error:
        log2.exception("write_to_txt: %s.", str(error))
        raise error


def write(prj_nm,json_data,datafram,counter,config_file_path,task_id,run_id,paths_data,
          file_path):
    """ function for ingesting data to S3 bucket based on the inputs in task json"""
    audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
    paths_data["audit_path"]+task_id+\
                '_audit_'+run_id+'.json'
    try:
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        #importing audit function from orchestrate script
        from engine_code import audit
        log2.info("ingest data to S3 initiated")
        conn = establish_conn(json_data,'target',config_file_path)
        status="Pass"
        csv_buf = StringIO()
        if counter == 1:
            datafram.to_csv(csv_buf, header=True, index=False)
            csv_buf.seek(0)
            conn.put_object(Bucket=json_data["task"]["target"]["bucket_name"],Body=
            csv_buf.getvalue(), Key=json_data["task"]["file_path"]+'/test.csv')
        else:
            datafram.to_csv(csv_buf, header=False, index=False)
            csv_buf.seek(0)
            conn.put_object(Bucket='pg-mystoreinventory-6135', Body=csv_buf.getvalue(),
            Key=json_data["task"]["file_path"]+'/test.csv')
        return status
    except Exception as error:
        write_to_txt(task_id,'FAILED',file_path)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        log2.exception("write() is %s", str(error))
        raise error

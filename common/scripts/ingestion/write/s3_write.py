""" script for writing data to S3"""
import logging
from datetime import datetime
import os
import sys
import io
import pandas as pd
import boto3
from utility import get_config_section,decrypt

log2 = logging.getLogger('log2')
folder_timestamp = datetime.now().strftime("%Y%m%d")
file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
CSV = '.csv'
XLSX = '.xlsx'
JSON = '.json'
PARQUET ='.parquet'
XML = '.xml'
REPLACE_OPERATION= "S3 target operation is REPLACE \n target path: %s"
APPEND_OPERATION="S3 target operation is APPEND"
OBJECT_EXISTS ="%s object already exists in the target location"
OBJECT_NOT_EXISTS = "%s object does not exists in the target location"
CREATE_OBJECT = "creating new object %s"

def establish_conn(json_data: dict, json_section: str,config_file_path:str):
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(config_file_path+json_data["task"][json_section]\
        ["connection_name"]+JSON)
        d_aws_access_key_id = decrypt(connection_details["aws_access_key_id"])
        d_aws_secret_access_key = decrypt(connection_details["aws_secret_access_key"])
        conn = boto3.client( service_name= 's3',region_name=
        connection_details["region_name"],aws_access_key_id=d_aws_access_key_id,
        aws_secret_access_key=d_aws_secret_access_key)
        logging.info("connection established")
        return conn,connection_details
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

def check_path(con,file_path,connection_details):
    "function to check whether table object in s3 exists or not"
    result = con.list_objects(Bucket=connection_details["bucket_name"], Prefix=file_path )
    exists=False
    if 'Contents' in result:
        exists=True
    return exists

def write_csv(json_data,datafram,conn,target_path,connection_details):
    """function for s3 write for csv filetype"""
    log2.info("s3 write operation for CSV filetype started")
    csv_buf = io.StringIO()
    datafram.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    if json_data["task"]["target"]["operation"] == "replace":
        #if operation is replace
        log2.info(REPLACE_OPERATION,target_path+CSV)
        conn.put_object(Bucket=connection_details["bucket_name"],Body=
        csv_buf.getvalue(), Key=target_path+CSV,ServerSideEncryption='AES256')
    elif json_data["task"]["target"]["operation"] == "append":
        # if operation is append
        log2.info(APPEND_OPERATION)
        object_exists = check_path(conn,target_path+CSV,connection_details)
        # checking whether object exists or not
        if object_exists is True:
            #if object in s3 already exists
            log2.info(OBJECT_EXISTS,\
            json_data["task"]["target"]["file_name"]+CSV)
            log2.info(CREATE_OBJECT,target_path+'_'+file_timestamp+CSV)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            csv_buf.getvalue(), Key=target_path+'_'+file_timestamp+CSV,
            ServerSideEncryption='AES256')
        else:
            #if object in s3 does not exists
            log2.info(OBJECT_NOT_EXISTS,\
            json_data["task"]["target"]["file_name"]+CSV)
            log2.info(CREATE_OBJECT,target_path+CSV)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            csv_buf.getvalue(), Key=target_path+CSV,ServerSideEncryption='AES256')

def write_excel(json_data,datafram,conn,target_path,connection_details):
    """function for s3 write for csv filetype"""
    log2.info("s3 write operation for EXCEL filetype started")
    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer: # pylint: disable=abstract-class-instantiated
            datafram.to_excel(writer,header=True, index=False)
        data = output.getvalue()
    if json_data["task"]["target"]["operation"] == "replace":
        log2.info(REPLACE_OPERATION,target_path+XLSX)
        conn.put_object(Bucket=connection_details["bucket_name"],Body=
        data, Key=target_path+XLSX,ServerSideEncryption='AES256')
    elif json_data["task"]["target"]["operation"] == "append":
        # if operation is append
        log2.info(APPEND_OPERATION)
        object_exists = check_path(conn,target_path+XLSX,connection_details)
        # checking whether object exists or not
        if object_exists is True:
            #if object in s3 already exists
            log2.info(OBJECT_EXISTS,\
            json_data["task"]["target"]["file_name"]+XLSX)
            log2.info(CREATE_OBJECT,target_path+'_'+file_timestamp+XLSX)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            data, Key=target_path+'_'+file_timestamp+XLSX,
            ServerSideEncryption='AES256')
        else:
            #if object in s3 does not exists
            log2.info(OBJECT_NOT_EXISTS,\
            json_data["task"]["target"]["file_name"]+XLSX)
            log2.info(CREATE_OBJECT,target_path+XLSX)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            data, Key=target_path+XLSX,ServerSideEncryption='AES256')

def write_json(json_data,datafram,conn,target_path,connection_details):
    """function for s3 write for JSON filetype"""
    log2.info("s3 write operation for JSON filetype started")
    json_buffer = io.StringIO()
    datafram.to_json(json_buffer,orient='records', index=True)
    if json_data["task"]["target"]["operation"] == "replace":
        #if operation is replace
        log2.info(REPLACE_OPERATION,target_path+JSON)
        conn.put_object(Bucket=connection_details["bucket_name"],Body=
        json_buffer.getvalue(), Key=target_path+JSON,ServerSideEncryption='AES256')
    elif json_data["task"]["target"]["operation"] == "append":
        # if operation is append
        log2.info(APPEND_OPERATION)
        object_exists = check_path(conn,target_path+JSON,connection_details)
        # checking whether object exists or not
        if object_exists is True:
            #if object in s3 already exists
            log2.info(OBJECT_EXISTS,\
            json_data["task"]["target"]["file_name"]+JSON)
            log2.info(CREATE_OBJECT,target_path+'_'+file_timestamp+JSON)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            json_buffer.getvalue(), Key=target_path+'_'+file_timestamp+JSON,
            ServerSideEncryption='AES256')
        else:
            #if object in s3 does not exists
            log2.info(OBJECT_NOT_EXISTS,\
            json_data["task"]["target"]["file_name"]+JSON)
            log2.info(CREATE_OBJECT,target_path+JSON)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            json_buffer.getvalue(), Key=target_path+JSON,ServerSideEncryption='AES256')

def write_parquet(json_data,datafram,conn,target_path,connection_details):
    """function for s3 write for parquet filetype"""
    log2.info("s3 write operation for PARQUET filetype started")
    parquet_buffer = io.BytesIO()
    datafram.astype(str).to_parquet(parquet_buffer,engine='auto', index=False)
    if json_data["task"]["target"]["operation"] == "replace":
        #if operation is replace
        log2.info(REPLACE_OPERATION,target_path+PARQUET)
        conn.put_object(Bucket=connection_details["bucket_name"],Body=
        parquet_buffer.getvalue(), Key=target_path+PARQUET,ServerSideEncryption='AES256')
    elif json_data["task"]["target"]["operation"] == "append":
        # if operation is append
        log2.info(APPEND_OPERATION)
        object_exists = check_path(conn,target_path+PARQUET,connection_details)
        # checking whether object exists or not
        if object_exists is True:
            #if object in s3 already exists
            log2.info(OBJECT_EXISTS,\
            json_data["task"]["target"]["file_name"]+PARQUET)
            log2.info(CREATE_OBJECT,target_path+'_'+file_timestamp+PARQUET)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            parquet_buffer.getvalue(), Key=target_path+'_'+file_timestamp+PARQUET,
            ServerSideEncryption='AES256')
        else:
            #if object in s3 does not exists
            log2.info(OBJECT_NOT_EXISTS,\
            json_data["task"]["target"]["file_name"]+PARQUET)
            log2.info(CREATE_OBJECT,target_path+PARQUET)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            parquet_buffer.getvalue(), Key=target_path+PARQUET,ServerSideEncryption='AES256')

def write_xml(json_data,datafram,conn,target_path,connection_details):
    """function for s3 write for XML filetype"""
    log2.info("s3 write operation for XML filetype started")
    xml_buffer = io.BytesIO()
    datafram.to_xml(xml_buffer,index=True)
    if json_data["task"]["target"]["operation"] == "replace":
        #if operation is replace
        log2.info(REPLACE_OPERATION,target_path+XML)
        conn.put_object(Bucket=connection_details["bucket_name"],Body=
        xml_buffer.getvalue(), Key=target_path+XML,ServerSideEncryption='AES256')
    elif json_data["task"]["target"]["operation"] == "append":
        # if operation is append
        log2.info(APPEND_OPERATION)
        object_exists = check_path(conn,target_path+XML,connection_details)
        # checking whether object exists or not
        if object_exists is True:
            #if object in s3 already exists
            log2.info(OBJECT_EXISTS,\
            json_data["task"]["target"]["file_name"]+XML)
            log2.info(CREATE_OBJECT,target_path+'_'+file_timestamp+XML)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            xml_buffer.getvalue(), Key=target_path+'_'+file_timestamp+XML,
            ServerSideEncryption='AES256')
        else:
            #if object in s3 does not exists
            log2.info(OBJECT_NOT_EXISTS,\
            json_data["task"]["target"]["file_name"]+XML)
            log2.info(CREATE_OBJECT,target_path+XML)
            conn.put_object(Bucket=connection_details["bucket_name"],Body=
            xml_buffer.getvalue(), Key=target_path+XML,ServerSideEncryption='AES256')

def write(json_data,datafram,config_file_path,task_id,run_id,paths_data,
          file_path,iter_value):
    """ function for ingesting data to S3 bucket based on the inputs in task json"""
    engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
    sys.path.insert(0, engine_code_path)
    #importing audit function from orchestrate script
    from engine_code import audit
    try:
        log2.info("ingest data to S3 initiated")
        conn,connection_details = establish_conn(json_data,'target',config_file_path)
        status="Pass"
        target_path = json_data["task"]["target"]["file_path"]+folder_timestamp+'/'+\
        json_data["task"]["target"]["file_name"]
        if json_data["task"]["target"]["file_type"]=="csv":
            write_csv(json_data,datafram,conn,target_path,connection_details)
        elif json_data["task"]["target"]["file_type"]=="excel":
            write_excel(json_data,datafram,conn,target_path,connection_details)
        elif json_data["task"]["target"]["file_type"]=="json":
            write_json(json_data,datafram,conn,target_path,connection_details)
        elif json_data["task"]["target"]["file_type"]=="parquet":
            write_parquet(json_data,datafram,conn,target_path,connection_details)
        elif json_data["task"]["target"]["file_type"]=="xml":
            write_xml(json_data,datafram,conn,target_path,connection_details)
        return status
    except Exception as error:
        write_to_txt(task_id,'FAILED',file_path)
        audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
        log2.exception("write() is %s", str(error))
        raise error

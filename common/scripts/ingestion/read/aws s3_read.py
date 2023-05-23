""" script for writing data to S3"""
import logging
import os
import importlib
import sys
import csv
import pandas as pd
import boto3

module = importlib.import_module("utility")
get_config_section = getattr(module, "get_config_section")
decrypt = getattr(module, "decrypt")

log2 = logging.getLogger('log2')
ITERATION='%s iteration'
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
        # log2.info("aws access key id %s", decrypt(connection_details["aws_access_key_id"]))
        d_aws_access_key_id = decrypt(connection_details["access_key"])
        d_aws_secret_access_key = decrypt(connection_details["secret_access_key"])
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

def read(json_data: dict,config_file_path,task_id,run_id,paths_data,file_path,
         iter_value,delimiter = ",", skip_header= 0,
        skip_footer= 0, quotechar = '"', escapechar = None):
    '''function to read s3 data'''
    try:
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        module = importlib.import_module("engine_code")
        audit = getattr(module, "audit")
        conn,connection_details = establish_conn(json_data,'target',config_file_path)
        bucket_name = connection_details["bucket_name"]
        # extension = '.'+json_data['task']['source']['file_type']
        path = json_data['task']['source']['file_path']+json_data['task']['source'][
            'file_name']

        # List all objects in the bucket
        response = conn.list_objects_v2(Bucket=bucket_name, Prefix=path)
        objects = response['Contents']
        # Filter the objects based on extension mentioned files
        all_files = [obj['Key'] for obj in objects if obj['Key'].lower().endswith('.csv')]
        log2.info("list of files which were read")
        log2.info(all_files)
        if all_files == []:
            log2.error("'%s' SOURCE FILE not found in the location",
            json_data["task"]["source"]["file_name"])
            write_to_txt(task_id,'FAILED',file_path)
            audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
            sys.exit()
        else:
            default_delimiter = delimiter if json_data["task"]["source"]["delimiter"]==" " else\
            json_data["task"]["source"]["delimiter"]
            default_skip_header = skip_header if json_data["task"]["source"]["skip_header"]\
            ==" " else json_data["task"]["source"]["skip_header"]
            default_skip_footer = skip_footer if json_data["task"]["source"]["skip_footer"]\
            ==" " else json_data["task"]["source"]["skip_footer"]
            default_quotechar = quotechar if json_data["task"]["source"]["quote_char"]==" " else\
            json_data["task"]["source"]["quote_char"]
            default_escapechar=escapechar if json_data["task"]["source"]["escape_char"]=="none" \
            else json_data["task"]["source"]["escape_char"]
            default_select_cols = None if json_data["task"]["source"]["select_columns"]==" " else\
            list(json_data["task"]["source"]["select_columns"].split(","))
            default_alias_cols = None if json_data["task"]["source"]["alias_columns"]==" " else\
            list(json_data["task"]["source"]["alias_columns"].split(","))
            default_encoding = "utf-8" if json_data["task"]["source"]["encoding"]==" " else\
            json_data["task"]["source"]["encoding"]

            # file_key = json_data['task']['source']['file_path']+json_data['task']['source'][
            #     'file_name']+'.'+json_data['task']['source']['file_type']

            # # Read the file from the S3 bucket
            # response = conn.get_object(Bucket=bucket_name, Key=file_key)
            # content = response['Body'].read().decode('utf-8')
            for file in all_files:
                # Call the `get_object()` method to retrieve the file object
                file_object = conn.get_object(Bucket=bucket_name, Key=file)
                # Count the number of records
                record_count = sum(1 for _ in csv.reader(file_object['Body']))
                audit(json_data, task_id,run_id,'SRC_RECORD_COUNT',record_count,
                iter_value)
                row_count = record_count-default_skip_header-default_skip_footer
                count1 = 0
                if json_data["task"]["source"]["select_columns"] != " " and \
                json_data["task"]["source"]["alias_columns"] != " ":
                    default_header = 0
                    # Use a `for` loop to read the file contents in chunks
                    # for chunk in file_object['Body'].iter_chunks(chunk_size=1000):
                    for chunk in csv.reader(file_object['Body'], names = default_alias_cols,
                    header = default_header,engine='python',sep = default_delimiter,
                    usecols = default_select_cols, skiprows = default_skip_header,nrows = row_count,
                    chunksize = json_data["task"]["source"]["chunk_size"],
                    quotechar = default_quotechar, escapechar = default_escapechar,
                    encoding = default_encoding):
                        count1 = 1 + count1
                        log2.info(ITERATION , str(count1))
                        yield chunk

                elif json_data["task"]["source"]["select_columns"] != " " and \
                json_data["task"]["source"]["alias_columns"] == " ":
                    default_header = 'infer' if json_data["task"]["source"]["alias_columns"]\
                    == " " else 0
                    for chunk in csv.reader(file_object['Body'], names = default_alias_cols,
                    header = default_header,sep = default_delimiter, usecols = default_select_cols,
                    skiprows = default_skip_header,nrows = row_count,
                    chunksize = json_data["task"]["source"]["chunk_size"],
                    quotechar = default_quotechar, escapechar = default_escapechar,
                    encoding = default_encoding):
                        count1 = 1 + count1
                        log2.info(ITERATION , str(count1))
                        yield chunk
                elif json_data["task"]["source"]["select_columns"] == " " and \
                json_data["task"]["source"]["alias_columns"] != " ":
                    default_header ='infer' if json_data["task"]["source"]["alias_columns"]\
                     == " " else 0
                    for chunk in csv.reader(file_object['Body'], names = default_alias_cols,
                    header = default_header,sep = default_delimiter, usecols = default_alias_cols,
                    skiprows = default_skip_header,nrows = row_count,
                    chunksize = json_data["task"]["source"]["chunk_size"],
                    quotechar = default_quotechar, escapechar = default_escapechar,
                    encoding = default_encoding):
                        count1 = 1 + count1
                        log2.info(ITERATION , str(count1))
                        yield chunk
                elif json_data["task"]["source"]["select_columns"] == " " and \
                json_data["task"]["source"]["alias_columns"] == " ":
                    default_header ='infer' if json_data["task"]["source"]["alias_columns"] ==\
                     " " else None
                    for chunk in csv.reader(file_object['Body'], names = default_alias_cols,
                    header = default_header,sep = default_delimiter, usecols = default_select_cols,
                    skiprows = default_skip_header,nrows = row_count,
                    chunksize = json_data["task"]["source"]["chunk_size"],
                    quotechar = default_quotechar, escapechar = default_escapechar,
                    encoding = default_encoding):
                        count1 = 1 + count1
                        log2.info(ITERATION , str(count1))
                        yield chunk
    except Exception as error:
        write_to_txt(task_id,'FAILED',file_path)
        audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
        log2.info("reading_csv() is %s", str(error))
        raise error

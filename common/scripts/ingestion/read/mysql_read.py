""" importing modules """
import logging
import os
import sys
import importlib
import sqlalchemy
import pymysql
import pandas as pd

module = importlib.import_module("utility")
get_config_section = getattr(module, "get_config_section")
decrypt = getattr(module, "decrypt")


log2 = logging.getLogger('log2')

def establish_conn(json_data: dict, json_section: str,config_file_path:str):
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(config_file_path+json_data["task"][json_section]
        ["connection_name"]+'.json')
        password = decrypt(connection_details["password"])
        conn1 = sqlalchemy.create_engine(f'mysql+pymysql://{connection_details["username"]}'
        f':{password.replace("@", "%40")}@{connection_details["hostname"]}'
        f':{int(connection_details["port"])}/{connection_details["database"]}', encoding='utf-8')
        # logging.info("connection established")
        return conn1
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

def read(json_data,config_file_path,task_id,run_id,paths_data,file_path,iter_value):
    """ function for reading data from mysql table"""
    try:
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        #importing audit function from orchestrate script
        module1 = importlib.import_module("engine_code")
        audit = getattr(module1, "audit")
        conn3 = establish_conn(json_data, 'source',config_file_path)
        log2.info('reading data from mysql started')
        connection = conn3.raw_connection()
        cursor = connection.cursor()
        count1 = 0
        if json_data["task"]["source"]["query"] == " ":
            log2.info("reading from mysql table: %s",
            json_data["task"]["source"]["table_name"])
            sql = f'SELECT count(0) from {json_data["task"]["source"]["table_name"]};'
            cursor.execute(sql)
            myresult = cursor.fetchall()
            audit(json_data, task_id,run_id,'SRC_RECORD_COUNT',myresult[-1][-1],
            iter_value)
            log2.info('the number of records present in source table before ingestion:%s',
            myresult[-1][-1])
            default_columns = None if json_data["task"]["source"]["select_columns"]==" "\
            else list(json_data["task"]["source"]["select_columns"].split(","))
            for query in pd.read_sql_table(json_data["task"]["source"]["table_name"], conn3,\
            columns = default_columns, \
            chunksize = json_data["task"]["source"]["chunk_size"]):
                count1+=1
                log2.info('%s iteration' , str(count1))
                yield query
        else:
            log2.info("reading from sql query")
            sql = f'SELECT count(0) from ({json_data["task"]["source"]["query"]}) as d;'
            log2.info(sql)
            cursor.execute(sql)
            myresult = cursor.fetchall()
            audit(json_data, task_id,run_id,'SRC_RECORD_COUNT',myresult[-1][-1],
            iter_value)
            log2.info('the number of records present in source table before ingestion:%s',
            myresult[-1][-1])
            log2.info('sql_query: %s',json_data["task"]["source"]["query"])
            for query in pd.read_sql(json_data["task"]["source"]["query"],
            conn3, chunksize = json_data["task"]["source"]["chunk_size"]):
                count1+=1
                log2.info('%s iteration' , str(count1))
                yield query
        conn3.dispose()
        return True
    except pymysql.err.ProgrammingError: #to handle table not found issue
        log2.error("the table name or connection specified in the task is incorrect/doesnot exists")
        write_to_txt(task_id,'FAILED',file_path)
        audit(json_data, task_id,run_id,'STATUS','FAILED',
        iter_value)
        sys.exit()
    except Exception as error:
        write_to_txt(task_id,'FAILED',file_path)
        audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
        log2.exception("read_data_from_mysql() is %s", str(error))
        raise error

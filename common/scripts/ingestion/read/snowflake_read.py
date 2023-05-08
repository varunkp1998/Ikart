""" script for reading data from snowflake table"""
import logging
import sys
import os
import pandas as pd
import sqlalchemy
from utility import get_config_section,decrypt
from snowflake.connector.errors import ProgrammingError

log2 = logging.getLogger('log2')

def establish_conn(json_data: dict, json_section: str,connection_file_path:str):
    """establishes connection for the snowflake database
       you pass it through the json"""
    try:
        connection_details =get_config_section(connection_file_path+json_data["task"][json_section]\
        ["connection_name"]+'.json')
        # print(connection_details["username"])
        password = decrypt(connection_details["password"])
        conn1= sqlalchemy.create_engine(f'snowflake://{connection_details["username"]}'
        f':{password.replace("@", "%40")}@{connection_details["account"]}/'
        f':{connection_details["database"]}/{json_data["task"]["source"]["schema"]}'
        f'?warehouse={connection_details["warehouse"]}&role={connection_details["role"]}')
        log2.info("connection established")
        # log2.info("connection established")
        return conn1,connection_details
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

def read(json_data,connection_file_path,task_id,run_id,paths_data,file_path,iter_value):
    """ function for reading data from snowflake table"""
    try:
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        #importing audit function from orchestrate script
        from engine_code import audit
        conn3,conn_details = establish_conn(json_data, 'source',connection_file_path)
        log2.info('reading data from snowflake started')
        connection = conn3.raw_connection()
        cursor = connection.cursor()
        count1 = 0
        if json_data["task"]["source"]["query"] == " ":
            #if query is empty it will go to table name execution
            log2.info("reading from snowflake table: %s",
            json_data["task"]["source"]["table_name"])
            schema_name =conn_details["database"]+'.'+ json_data["task"]["source"]["schema"]
            sql = f'SELECT count(0) from {schema_name}.{json_data["task"]["source"]["table_name"]};'
            cursor.execute(sql)
            myresult = cursor.fetchall()
            audit(json_data, task_id,run_id,'SRC_RECORD_COUNT',myresult[-1][-1],
            iter_value)
            log2.info('the number of records present in source table before ingestion:%s',
            myresult[-1][-1])
            query = (f'select * from {conn_details["database"]}.'
                     f'{json_data["task"]["source"]["schema"]}.'
                     f'{json_data["task"]["source"]["table_name"]}')
            # log2.info(conn_details["database"])
            conn3.execution_options(autocommit=True).execute(query)
            for query in pd.read_sql(query,conn3,\
            chunksize = json_data["task"]["source"]["chunk_size"]):
                count1+=1
                log2.info('%s iteration' , str(count1))
                # log2.info(query)
                yield query
        else:
            #if query is  not empty, goes for query execution
            log2.info("reading from sql query")
            sql = f'SELECT count(0) from ({json_data["task"]["source"]["query"]}) as d;'
            # log2.info(sql)
            cursor.execute(sql)
            myresult = cursor.fetchall()
            audit(json_data, task_id,run_id,'SRC_RECORD_COUNT',myresult[-1][-1],
            iter_value)
            log2.info('sql_query: %s',json_data["task"]["source"]["query"])
            for query in pd.read_sql(json_data["task"]["source"]["query"],
            conn3, chunksize = json_data["task"]["source"]["chunk_size"]):
                count1+=1
                log2.info('%s iteration' , str(count1))
                yield query
        conn3.dispose()
        return True
    except ProgrammingError : #to handle table not found issue
        log2.error("the table name or connection specified in the command is incorrect")
        write_to_txt(task_id,'FAILED',file_path)
        audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
        sys.exit()
    # except sqlalchemy.exc.OperationalError:
    #     log2.error("The details provided inside the connection file path is incorrect")
    #     status = 'FAILED'
    #     write_to_txt(task_id,status,run_id,paths_data)
    #     sys.exit()
    except Exception as error:
        write_to_txt(task_id,'FAILED',file_path)
        audit(json_data, task_id,run_id,'STATUS','FAILED',iter_value)
        log2.exception("read_data_from_snowflake() is %s", str(error))
        raise error

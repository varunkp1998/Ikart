""" script for writing data from mysql table"""
import sys
import logging
import os
from datetime import datetime
from sqlalchemy.exc import OperationalError
import sqlalchemy
import pandas as pd
import pymysql
from utility import get_config_section,decrypt

log2 = logging.getLogger('log2')

def establish_conn(json_data: dict, json_section: str,config_file_path:str) -> bool:
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(config_file_path+json_data["task"][json_section]\
        ["connection_name"]+'.json', json_data["task"][json_section]["connection_name"])
        password = decrypt(connection_details["password"])
        conn = sqlalchemy.create_engine(f'mysql+pymysql://{connection_details["user"]}'
        f':{password.replace("@", "%40")}@{connection_details["host"]}'
        f':{int(connection_details["port"])}/{connection_details["database"]}', encoding='utf-8')
        # logging.info("connection established")
        return conn,connection_details
    except Exception as error:
        log2.exception("establish_conn() is %s", str(error))
        raise error

def db_table_exists(conn: dict, tablename: str)-> bool:
    """ function for checking whether a table exists or not in mysql """
    try:
        # checking whether the table exists in database or not
        sql = f"select table_name from information_schema.tables where table_name='{tablename}'"
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        # print(bool(cursor.rowcount))
        return bool(cursor.rowcount)
        # return results of sql query from conn as a pandas dataframe
        # results_df = pd.read_sql_query(sql, conn)
        # returns True if table exists else False
        # return bool(len(results_df))
    except Exception as error:
        log2.exception("db_table_exists() is %s", str(error))
        raise error

def create(json_data: dict, conn, datafram,conn_details:str)-> bool:
    """if table is not present , it will create"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["table_name"]) is False:
            log2.info('%s does not exists so creating a new table',
            json_data["task"]["target"]["table_name"])
            if json_data["task"]["target"]["audit_columns"] == "active":
                datafram['CRTD_BY']=conn_details["user"]
                datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datafram['UPDT_BY']= " "
                datafram['UPDT_DTTM']= " "
                # dataframe = dataframe.fillna("etl_user")
                datafram.to_sql(json_data["task"]["target"]["table_name"], conn,
                index = False, if_exists = "append")
                log2.info("mysql ingestion completed")
            else:
                log2.info("data ingesting without audit columns")
                datafram.to_sql(json_data["task"]["target"]["table_name"], conn,
                index = False, if_exists = "append")
                log2.info("mysql ingestion completed")
        else:
            # if table exists, it will say table is already present, give new name to create
            log2.error('%s already exists, so give a new table name to create',
            json_data["task"]["target"]["table_name"])
            # sys.exit()
            return "Fail"
    except OperationalError as error:
        if 'Duplicate column name' in str(error):
            log2.error("there are duplicate column names in the target table")
            # status = 'FAILED'
            # write_to_txt(prj_nm,task_id,'FAILED',run_id,paths_data)
            # audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
            # # return "Fail"
            sys.exit()
        else:
            log2.info("else")
            # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
            log2.exception("create() is %s", str(error))
            raise error

def append(json_data: dict, conn: dict, dataframe, conn_details) -> bool:
    """if table exists, it will append"""
    try:
        # print(json_data["task"]["target"]["table_name"])
        if db_table_exists(conn, json_data["task"]["target"]["table_name"]) is True:
            log2.info("%s table exists, started appending the data to table",
            json_data["task"]["target"]["table_name"])
            if json_data["task"]["target"]["audit_columns"] == "active":
                dataframe['CRTD_BY']=conn_details["user"]
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                dataframe.to_sql(json_data["task"]["target"]["table_name"], conn, index = False,
                if_exists = "append")
                log2.info("mysql ingestion completed")
            else:
                dataframe.to_sql(json_data["task"]["target"]["table_name"], conn, index = False,
                if_exists = "append")
                log2.info("mysql ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            # create table first or give table name that exists to append data
            log2.error('%s does not exists, so create table first',
            json_data["task"]["target"]["table_name"])
            # audit(audit_json_path,json_data, task_id,'STATUS','COMPLETED')
            # sys.exit()
            return "Fail"
    except OperationalError as error:
        if "Unknown column 'CRTD_BY' in 'field list'" in str(error):
            log2.error("audit columns not found in the table previously to append")
            # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
            # sys.exit()
            return "Fail"
        else:
            # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
            log2.exception("append() is %s", str(error))
            raise error

def replace(json_data: dict, conn: dict, dataframe,counter: int, conn_details) -> bool:
    """if table exists, it will drop and replace data"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["table_name"]) is True:
            if counter == 1:
                log2.info("%s table exists, started replacing the table",
                json_data["task"]["target"]["table_name"])
                replace_query = sqlalchemy.text(f'DROP TABLE '
                f'{json_data["task"]["target"]["table_name"]}')
                conn.execution_options(autocommit=True).execute(replace_query)
                log2.info(" table replace finished, started inserting data into "
                 "%s table", json_data["task"]["target"]["table_name"])
                if json_data["task"]["target"]["audit_columns"] == "active":
                    dataframe['CRTD_BY']=conn_details["user"]
                    dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    dataframe['UPDT_BY']= " "
                    dataframe['UPDT_DTTM']= " "
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
                else:
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
            else:
                if json_data["task"]["target"]["audit_columns"] == "active":
                    dataframe['CRTD_BY']=conn_details["user"]
                    dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    dataframe['UPDT_BY']= " "
                    dataframe['UPDT_DTTM']= " "
                    # dataframe = dataframe.fillna("etl_user")
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
                else:
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            log2.error('%s does not exists, give correct table name',
            json_data["task"]["target"]["table_name"])
            # audit(audit_json_path,json_data, task_id,'STATUS','COMPLETED')
            # sys.exit()
            return "Fail"
    except Exception as error:
        # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
        log2.exception("replace() is %s", str(error))
        raise error

def truncate(json_data: dict, conn: dict,dataframe,counter: int, conn_details) -> bool:
    """if table exists, it will truncate"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["table_name"]) is True:
            if counter == 1:
                log2.info("%s table exists, started truncating the table",
                json_data["task"]["target"]["table_name"])
                truncate_query = sqlalchemy.text(f'TRUNCATE TABLE '
                f'{json_data["task"]["target"]["table_name"]}')
                conn.execution_options(autocommit=True).execute(truncate_query)
                log2.info("mysql truncating table finished, started inserting data into "
                "%s table", json_data["task"]["target"]["table_name"])
                if json_data["task"]["target"]["audit_columns"] == "active":
                    dataframe['CRTD_BY']=conn_details["user"]
                    dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    dataframe['UPDT_BY']= " "
                    dataframe['UPDT_DTTM']= " "
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
                else:
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
            else:
                if json_data["task"]["target"]["audit_columns"] == "active":
                    dataframe['CRTD_BY']=conn_details["user"]
                    dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    dataframe['UPDT_BY']= " "
                    dataframe['UPDT_DTTM']= " "
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
                else:
                    dataframe.to_sql(json_data["task"]["target"]["table_name"], conn,
                    index = False, if_exists = "append")
                    log2.info("mysql ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            log2.error('%s does not exists, give correct table name to truncate',
            json_data["task"]["target"]["table_name"])
            # audit(audit_json_path,json_data, task_id,'STATUS','COMPLETED')
            # sys.exit()
            return "Fail"
    except OperationalError as error:
        if "Unknown column 'CRTD_BY' in 'field list'" in str(error):
            # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
            log2.error("audit columns not found in the table previously"
            "to insert data after truncate")
            # sys.exit()
            return "Fail"
        else:
            # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
            log2.exception("append() is %s", str(error))
            raise error

def drop(json_data: dict, conn: dict) -> bool:
    """if table exists, it will drop"""
    try:
        if db_table_exists(conn, json_data["task"]["target"]["table_name"]) is True:
            log2.info("%s table exists, started dropping the table",
            json_data["task"]["target"]["table_name"])
            drop_query = sqlalchemy.text(f'DROP TABLE '
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(drop_query)
            log2.info("mysql dropping table completed")
            # sys.exit()
            return "drop_Pass"
            # log2.info(" table drop finished, started inserting data into
            #  %s table", json_data["table"])
            # for chunk in dataframe:
            #     chunk.to_sql(json_data["table"], conn, schema = json_data["schema"],
            #     index = False, if_exists = "append"
            # )
        else:
            # if table is not there, then it will say table does not exist
            log2.error('%s does not exists, give correct table name to drop',
            json_data["task"]["target"]["table_name"])
            # audit(audit_json_path,json_data, task_id,'STATUS','COMPLETED')
            # sys.exit()
            return "Fail"
    except Exception as error:
        # audit(audit_json_path,json_data, task_id,'STATUS','FAILED')
        log2.exception("drop() is %s", str(error))
        raise error

def write_to_txt(prj_nm,task_id,status,run_id,paths_data):
    """Generates a text file with statuses for orchestration"""
    place=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
    paths_data["status_txt_file_path"]+run_id+".txt"
    is_exist = os.path.exists(place )
    if is_exist is True:
        data_fram =  pd.read_csv(place, sep='\t')
        data_fram.loc[data_fram['task_name']==task_id, 'Job_Status'] = status
        data_fram.to_csv(place ,mode='w', sep='\t',index = False, header=True)
    else:
        log2.info("pipeline txt file does not exist")


def write(prj_nm,json_data: dict, datafram, counter: int,config_file_path:str,task_id,run_id,paths_data, pip_nm) -> bool:
    """ function for ingesting data to mysql based on the operation in json"""
    audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
    paths_data["audit_path"]+task_id+\
                '_audit_'+run_id+'.json'
    try:
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        #importing audit function from orchestrate script
        from engine_code import audit
        log2.info("ingest data to mysql db initiated")
        conn,conn_details = establish_conn(json_data,'target',config_file_path)
        status="Pass"
        if json_data["task"]["target"]["operation"] == "create":
            if counter == 1:
                status=create(json_data, conn, datafram, conn_details)
            else:
                status=append(json_data, conn, datafram,conn_details)
        elif json_data["task"]["target"]["operation"] == "append":
            status=append(json_data, conn, datafram,conn_details)
        elif json_data["task"]["target"]["operation"] == "truncate":
            status=truncate(json_data, conn, datafram, counter,conn_details)
        elif json_data["task"]["target"]["operation"] == "drop":
            status=drop(json_data, conn)
            # log2.info(status)
        elif json_data["task"]["target"]["operation"] == "replace":
            status=replace(json_data, conn, datafram, counter,conn_details)
        elif json_data["task"]["target"]["operation"] not in ("create", "append",
            "truncate", "drop","replace"):
            log2.error("give propper input for operation condition")
            status = "Fail"
            # sys.exit()
        if json_data["task"]["target"]["operation"] != "drop":
            connection = conn.raw_connection()
            cursor = connection.cursor()
            sql = f'SELECT count(0) from  {json_data["task"]["target"]["table_name"]};'
            cursor.execute(sql)
            myresult = cursor.fetchall()
            audit(audit_json_path,json_data, task_id,run_id,'TRGT_RECORD_COUNT',myresult[-1][-1])
            log2.info('the number of records present in target table after ingestion:%s',
            myresult[-1][-1])
        conn.dispose()
        # return myresult[-1][-1]
        return status
    # except OperationalError:
    #     # log2.error("there are duplicate column names in the target table")
    #     status = 'FAILED'
    #     write_to_txt(prj_nm,task_id,status,run_id,paths_data)
    #     audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
    #     sys.exit()
    # except pymysql.err.ProgrammingError: #to handle table not found issue
    #     log2.error("the table name or connection specified in the task is incorrect/doesnot exists")
    #     status = 'FAILED'
    #     write_to_txt(prj_nm,task_id,status,run_id,paths_data)
    #     audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
    #     sys.exit()
    except Exception as error:
        write_to_txt(prj_nm,task_id,'FAILED',run_id,paths_data)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        log2.exception("write() is %s", str(error))
        raise error

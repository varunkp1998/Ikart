""" script for writing data to snowflake table"""
import sys
import logging
from datetime import datetime
import os
import sqlalchemy
from sqlalchemy.exc import ProgrammingError
import pandas as pd
from snowflake.connector.errors import ProgrammingError
from utility import get_config_section,decrypt

log2 = logging.getLogger('log2')

def db_table_exists(conn: dict,database: str,schema: str, table_name: str)-> bool:
    """ function for checking whether a table exists or not in snowflake """
    # checking whether the table exists in database or not
    try:
        sql = f"select table_schema, table_name from {database}.information_schema.tables "\
            f"where table_name ='{table_name.upper()}'  and table_schema = '{schema.upper()}'"
        # print(sql)
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        # print(bool(cursor.rowcount))
        return bool(cursor.rowcount)
        # results_df = pd.read_sql_query(sql, conn)
        # print(results_df)
        # print(bool(len(results_df)))
        # return bool(len(results_df))
    except Exception as error:
        log2.exception("db_table_exists() is %s", str(error))
        raise error
    # returns True if table exists else False
    # print(bool(len(results_df)))
    # return bool(len(results_df))

def establish_conn(json_data: dict, json_section: str,config_file_path:str) -> bool:
    """establishes connection for the snowflake database
       you pass it through the json"""
    try:
        connection_details =  get_config_section(config_file_path+json_data["task"][json_section]\
        ["connection_name"]+'.json', json_data["task"][json_section]["connection_name"])
        password = decrypt(connection_details["password"])
        conn = sqlalchemy.create_engine(f'snowflake://{connection_details["user"]}'
        f':{password.replace("@", "%40")}@{connection_details["account"]}/'
        f':{connection_details["database"]}/{connection_details["schema"]}'
        f'?warehouse={connection_details["warehouse"]}&role={connection_details["role"]}')
        # log2.info("connection established")
        return conn,connection_details
    except Exception as error:
        log2.exception("establish_conn() is %s", str(error))
        raise error

def create(json_data: dict, conn, datafram, conn_details) -> bool:
    """if table is not present , it will create"""
    try:
        if db_table_exists(conn ,conn_details["database"],json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is False:
            log2.info('%s does not exists so creating a new table',\
            json_data["task"]["target"]["table_name"])
            schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
            if json_data["task"]["target"]["audit_columns"] == "active":
                datafram['CRTD_BY']=conn_details["user"]
                datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datafram['UPDT_BY']= " "
                datafram['UPDT_DTTM']= " "
                log2.info("data ingesting with audit columns")
                # log2.info(datafram['ORDERDATE'])
                # schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
                datafram.to_sql(json_data["task"]["target"]["table_name"], conn,\
                schema = schema_name,index = False, if_exists = "append")
                log2.info("snowflake ingestion completed")
            else:
                log2.info("data ingesting without audit columns")
                datafram.to_sql(json_data["task"]["target"]["table_name"], conn,\
                schema = schema_name,index = False, if_exists = "append")
                log2.info("snowflake ingestion completed")
        else:
            # if table exists, it will say table is already present, give new name to create
            log2.error('%s already exists, so give a new table name to create',
            json_data["task"]["target"]["table_name"])
            # sys.exit()
            return "Fail"
    except Exception as error:
        log2.exception("create() is %s", str(error))
        raise error

def append(json_data: dict, conn: dict, datafram,conn_details) -> bool:
    """if table exists, it will append"""
    try:
        if db_table_exists(conn,conn_details["database"] ,json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is True:
            log2.info("%s table exists, started appending the data to table",
            json_data["task"]["target"]["table_name"])
            schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
            if json_data["task"]["target"]["audit_columns"] == "active":
                datafram['CRTD_BY']=conn_details["user"]
                datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datafram['UPDT_BY']= " "
                datafram['UPDT_DTTM']= " "
                log2.info("data ingesting with audit columns")
                # schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
                datafram.to_sql(json_data["task"]["target"]["table_name"], conn,
                schema = schema_name ,index = False, if_exists = "append")
                log2.info("snowflake ingestion completed")
            else:
                log2.info("data ingesting without audit columns")
                datafram.to_sql(json_data["task"]["target"]["table_name"], conn,
                schema = schema_name ,index = False, if_exists = "append")
                log2.info("snowflake ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            # create table first or give table name that exists to append data
            log2.error('%s does not exists, so create table first',\
            json_data["task"]["target"]["table_name"])
            # sys.exit()
            return "Fail"
    except ProgrammingError as error:
        if 'column "CRTD_BY" of relation' in str(error):
            log2.error("audit columns not found in the table previously to append")
            # sys.exit()
            return "Fail"
            # raise Exception("audit columns not found in the table previously") from error
        else:
            log2.exception("append() is %s", str(error))
            raise error

def truncate(json_data: dict, conn: dict,datafram,counter: int, conn_details) -> bool:
    """if table exists, it will truncate"""
    try:
        if db_table_exists(conn,conn_details["database"] , json_data["task"]["target"]["schema"],
            json_data["task"]["target"]["table_name"]) is True:
            schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
            if counter == 1:
                log2.info("%s table exists, started truncating the table",
                json_data["task"]["target"]["table_name"])
                truncate_query = sqlalchemy.text(f'TRUNCATE TABLE '
                f'{schema_name}.'
                f'{json_data["task"]["target"]["table_name"]}')
                conn.execution_options(autocommit=True).execute(truncate_query)
                log2.info("snowflake truncating table finished, started inserting data into "
                "%s table", json_data["task"]["target"]["table_name"])
                if json_data["task"]["target"]["audit_columns"] == "active":
                    datafram['CRTD_BY']=conn_details["user"]
                    datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    datafram['UPDT_BY']= " "
                    datafram['UPDT_DTTM']= " "
                    log2.info("data ingesting with audit columns")
                    datafram.to_sql(json_data["task"]["target"]["table_name"],conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
                else:
                    log2.info("data ingesting without audit columns")
                    datafram.to_sql(json_data["task"]["target"]["table_name"],conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
            else:
                if json_data["task"]["target"]["audit_columns"] == "active":
                    datafram['CRTD_BY']=conn_details["user"]
                    datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    datafram['UPDT_BY']= " "
                    datafram['UPDT_DTTM']= " "
                    log2.info("data ingesting with audit columns")
                    datafram.to_sql(json_data["task"]["target"]["table_name"],conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
                else:
                    log2.info("data ingesting without audit columns")
                    datafram.to_sql(json_data["task"]["target"]["table_name"], conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            log2.error('%s does not exists, give correct table name to truncate',
            json_data["task"]["target"]["table_name"])
            # sys.exit()
            return "Fail"
    except ProgrammingError as error:
        if 'column "inserted_by" of relation' in str(error):
            log2.error("audit columns not found in the table previously to insert data")
            # sys.exit()
            return "Fail"
            # raise Exception("audit columns not found in the table previously") from error
        else:
            log2.exception("append() is %s", str(error))
            raise error

def drop(json_data: dict, conn: dict,conn_details) -> bool:
    """if table exists, it will drop"""
    try:
        if db_table_exists(conn,conn_details["database"], json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is True:
            log2.info("%s table exists, started dropping the table",
            json_data["task"]["target"]["table_name"])
            schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
            drop_query = sqlalchemy.text(f'DROP TABLE {schema_name}.'
            f'{json_data["task"]["target"]["table_name"]}')
            conn.execution_options(autocommit=True).execute(drop_query)
            log2.info("snowflake dropping table completed")
            # sys.exit()
            return "Fail"
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
            # sys.exit()
            return "Fail"
    except Exception as error:
        log2.exception("drop() is %s", str(error))
        raise error

def replace(json_data: dict, conn: dict, datafram,counter: int, conn_details: list) -> bool:
    """if table exists, it will drop and replace data"""
    try:
        if db_table_exists(conn,conn_details["database"], json_data["task"]["target"]["schema"],
        json_data["task"]["target"]["table_name"]) is True:
            schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
            if counter == 1:
                log2.info("%s table exists, started replacing the table",
                json_data["task"]["target"]["table_name"])
                schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
                replace_query = sqlalchemy.text(f'DROP TABLE '
                f'{schema_name}.'
                f'{json_data["task"]["target"]["table_name"]}')
                conn.execution_options(autocommit=True).execute(replace_query)
                if json_data["task"]["target"]["audit_columns"] == "active":
                    datafram['CRTD_BY']=conn_details["user"]
                    datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    datafram['UPDT_BY']= " "
                    datafram['UPDT_DTTM']= " "
                    log2.info("data ingesting with audit columns")
                    # dataframe = dataframe.fillna("etl_user")
                    datafram.to_sql(json_data["task"]["target"]["table_name"],conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
                else:
                    log2.info("data ingesting without audit columns")
                    datafram.to_sql(json_data["task"]["target"]["table_name"], conn, schema =
                    schema_name,
                    index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
            else:
                if json_data["task"]["target"]["audit_columns"] == "active":
                    datafram['CRTD_BY']=conn_details["user"]
                    datafram['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    datafram['UPDT_BY']= " "
                    datafram['UPDT_DTTM']= " "
                    log2.info("data ingesting with audit columns")
                    # dataframe = dataframe.fillna("etl_user")
                    datafram.to_sql(json_data["task"]["target"]["table_name"],conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
                else:
                    log2.info("data ingesting without audit columns")
                    datafram.to_sql(json_data["task"]["target"]["table_name"], conn, schema =
                    schema_name,index = False, if_exists = "append")
                    log2.info("snowflake ingestion completed")
        else:
            # if table is not there, then it will say table does not exist
            log2.error('%s does not exists, give correct table name',\
            json_data["task"]["target"]["table_name"])
            # sys.exit()
            return "Fail"
    except Exception as error:
        log2.exception("replace() is %s", str(error))
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

def write(prj_nm,json_data, datafram ,counter,config_file_path,task_id,run_id,paths_data, pip_nm) -> bool:
    """ function for ingesting data to snowflake based on the operation in json"""
    audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
    paths_data["audit_path"]+task_id+\
                '_audit_'+run_id+'.json'
    # if pip_nm == "-9999":
    #     #declaring audit json path for auditing purpose
    #     audit_json_path = paths_data["folder_path"] +paths_data["audit_path"]+task_id+\
    #             '_audit_'+run_id+'.json'
    # else:
    #     audit_json_path = paths_data["folder_path"] +paths_data["audit_path"]+pip_nm+\
    #             '_audit_'+run_id+'.json'
    try:
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        #importing audit function from orchestrate script
        from engine_code import audit
        log2.info("ingest data to snowflake db initiated")
        conn2,conn_details = establish_conn(json_data, 'target',config_file_path)
        status="Pass"
        if json_data["task"]["target"]["operation"] == "create":
            if counter == 1:
                status=create(json_data, conn2, datafram,conn_details)
                # print(create)
            else:
                status=append(json_data, conn2, datafram,conn_details)
        elif json_data["task"]["target"]["operation"] == "append":
            status=append(json_data, conn2, datafram,conn_details)
        elif json_data["task"]["target"]["operation"] == "truncate":
            status=truncate(json_data, conn2, datafram, counter,conn_details)
        elif json_data["task"]["target"]["operation"] == "drop":
            status=drop(json_data, conn2,conn_details)
        elif json_data["task"]["target"]["operation"] == "replace":
            status=replace(json_data, conn2, datafram, counter,conn_details)
        elif json_data["task"]["target"]["operation"] not in ("create", "append","truncate",
            "drop","replace"):
            log2.error("give proper input for operation to be performed on table")
            sys.exit()
        connection = conn2.raw_connection()
        cursor = connection.cursor()
        schema_name =conn_details["database"]+'.'+ json_data["task"]["target"]["schema"]
        sql = f'SELECT count(0) from  {schema_name}.{json_data["task"]["target"]["table_name"]};'
        cursor.execute(sql)
        myresult = cursor.fetchall()
        audit(audit_json_path,json_data, task_id,run_id,'TRGT_RECORD_COUNT',myresult[-1][-1])
        log2.info('the number of records present in target table after ingestion:%s',
        myresult[-1][-1])
        conn2.dispose()
        return status
    except ProgrammingError : #to handle table not found issue
        log2.error("the table or connection specified in the command is incorrect")
        status = 'FAILED'
        write_to_txt(prj_nm,task_id,status,run_id,paths_data)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        sys.exit()
    except Exception as error:
        status = 'FAILED'
        write_to_txt(prj_nm,task_id,status,run_id,paths_data)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        log2.exception("ingest_data_to_snowflake() is %s", str(error))
        raise error

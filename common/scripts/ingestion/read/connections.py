"""script for connecting different databases and s3"""
import importlib
import logging
import sqlalchemy
import boto3


log2 = logging.getLogger('log2')
module = importlib.import_module("utility")
get_config_section = getattr(module, "get_config_section")
decrypt = getattr(module, "decrypt")
JSON = '.json'
CONN_ESTB_MSG = "establish_conn() is %s"

def establish_conn_for_s3(json_data: dict, json_section: str,config_file_path:str):
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(config_file_path+json_data["task"][json_section]\
        ["connection_name"]+JSON)
        d_aws_access_key_id = decrypt(connection_details["access_key"])
        d_aws_secret_access_key = decrypt(connection_details["secret_access_key"])
        conn = boto3.client( service_name= 's3',region_name=
        connection_details["region_name"],aws_access_key_id=d_aws_access_key_id,
        aws_secret_access_key=d_aws_secret_access_key)
        log2.info("connection established")
        return conn,connection_details
    except Exception as error:
        log2.exception(CONN_ESTB_MSG, str(error))
        raise error

def establish_conn_for_snowflake(json_data: dict, json_section: str,config_file_path:str):
    """establishes connection for the snowflake database
       you pass it through the json"""
    try:
        connection_details =  get_config_section(config_file_path+json_data["task"][json_section]\
        ["connection_name"]+JSON)
        password = decrypt(connection_details["password"])
        conn = sqlalchemy.create_engine(f'snowflake://{connection_details["username"]}'
        f':{password.replace("@", "%40")}@{connection_details["account"]}/'
        f':{connection_details["database"]}/{json_data["task"][json_section]["schema"]}'
        f'?warehouse={connection_details["warehouse"]}&role={connection_details["role"]}')
        return conn,connection_details
    except Exception as error:
        log2.exception(CONN_ESTB_MSG, str(error))
        raise error

def establish_conn_for_mysql(json_data: dict, json_section: str,config_file_path:str):
    """establishes connection for the mysql database
       you pass it through the json"""
    try:
        connection_details = get_config_section(config_file_path+json_data["task"][json_section]
        ["connection_name"]+JSON)
        password = decrypt(connection_details["password"])
        conn1 = sqlalchemy.create_engine(f'mysql+pymysql://{connection_details["username"]}'
        f':{password.replace("@", "%40")}@{connection_details["hostname"]}'
        f':{int(connection_details["port"])}/{connection_details["database"]}', encoding='utf-8')
        # logging.info("connection established")
        return conn1,connection_details
    except Exception as error:
        log2.exception(CONN_ESTB_MSG, str(error))
        raise error

""" importing modules """
import logging
import argparse
import sys
import json
import os
import uuid
import subprocess
import importlib
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import requests
import mysql.connector
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


JSON = ".json"
DOWNLOAD_FILE ='download.py'
DOWNLOAD_LOG_STATEMENT = "download.py file downloading completed"
MASTER_LOG_STATEMENT = "Master execution started"

def setup_logger(logger_name, log_file, level=logging.INFO):
    """Function to initiate logging for framework by creating logger objects"""
    try:
        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter('%(asctime)s | %(name)-10s | %(processName)-12s |\
        %(funcName)-22s | %(levelname)-5s | %(message)s')
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)
        logger.setLevel(level)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        logger.propagate = False
        logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
        logging.getLogger('great_expectations.experimental.datasources').setLevel(logging.WARNING)
        return logger
    except Exception as ex:
        logging.error('UDF Failed: setup_logger failed')
        raise ex

# def decrypt(edata):
#     """password decryption function"""
#     try:
#         crypto_key= '8ookgvdIiH2YOgBnAju6Nmxtp14fn8d3'
#         crypto_iv= 'rBEssDfxofOveRxR'
#         block_size=16

#         key = bytes(crypto_key, 'utf-8')
#         value = bytes(crypto_iv, 'utf-8')

#         # Add "=" padding back before decoding
#         edata = base64.urlsafe_b64decode(edata + '=' * (-len(edata) % 4))
#         aes = AES.new(key, AES.MODE_CBC, value)
#         return unpad(aes.decrypt(edata), block_size).decode("utf-8")
#     except Exception as err:
#         logging.exception("decrypt() is %s.", str(err))
#         raise err

def decrypt(data):
    '''function to decrypt the data'''
    KEY = b'8ookgvdIiH2YOgBnAju6Nmxtp14fn8d3'
    IV = b'rBEssDfxofOveRxR'
    aesgcm = AESGCM(KEY)
    decrypted = aesgcm.decrypt(IV, bytes.fromhex(data), None)
    return decrypted.decode('utf-8')

# reading the connection.json file and passing the connection details as dictionary
def get_config_section(config_path:str) -> dict:
    """reads the connection file and returns connection details as dict for
       connection name you pass it through the json
    """
    try:
        with open(config_path,'r', encoding='utf-8') as json_file:
            # print("fetching connection details")
            json_data = json.load(json_file)
            # print("reading connection details completed")
            return dict(json_data["connection_details"].items())
    except Exception as err:
        logging.exception("get_config_section() is %s.", str(err))
        raise err

def download_audit_conn_json(path_json_data:str):
    """Function to download audit config JSON from Github to server"""
    try:
        # print("downloading audit_conn.json from Github started..")
        audit_conn_json_path= os.path.expanduser(path_json_data["folder_path"])
        # print("path:%s",audit_conn_json_path)
        path_check = audit_conn_json_path+path_json_data["mysql_audit_json_conn_nm"]
        is_exist = os.path.exists(path_check)
        # print('audit_conn.json file exists: %s', is_exist)
        if is_exist is False:
            git_url=path_json_data["GH_audit_conn_json_path"]
            response1 = requests.get(git_url,timeout=60)
            if response1.status_code != 200:
                logging.error("The audit config json file %s DOES NOT exists in the GITHUB "
                "repository \n PROCESS got ABORTED",path_json_data["mysql_audit_json_conn_nm"])
                sys.exit()
            else:
                download_json = ['curl', '-o',audit_conn_json_path+path_json_data[
                "mysql_audit_json_conn_nm"], path_json_data["GH_audit_conn_json_path"]]
                subprocess.call(download_json)
    except Exception as err:
        logging.exception("error in download_pipeline_json %s.", str(err))
        raise err

def execute_query(paths_json_data, pip_nm):
    """gets audit status from audit table for given pipeline"""
    try:
        download_audit_conn_json(paths_json_data)
        config_file_path = os.path.expanduser(paths_json_data["folder_path"])+\
        paths_json_data["mysql_audit_json_conn_nm"]
        conn_details = get_config_section(config_file_path)
        pass_word = decrypt(conn_details["password"])
        mydb = mysql.connector.connect(
            host=conn_details["hostname"],
            user=conn_details["username"],
            password=pass_word,
            database=conn_details["database"])
        # Create cursor object
        cursor = mydb.cursor()
        query = f"select run_id,iteration,audit_value from tbl_etl_audit where\
        `task/pipeline_name` = '{pip_nm}' ORDER BY process_dttm desc limit 1;"
        # main_logger.info(query)
        # Execute query
        cursor.execute(query)
        # Get results
        result = cursor.fetchall()
        # Close cursor and database connection
        cursor.close()
        mydb.close()
        # Return result
        return result
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Something is wrong with audit config username or password")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            logging.error("Database does not exist")
        else:
            logging.error("Error:%s ", err)
        raise err
    except Exception as error1:
        logging.exception("execute_query() is %s.", str(error1))
        raise error1

if __name__ == "__main__":
    try:
        ######### Setting variables #########################################
        parser = argparse.ArgumentParser()
        parser.add_argument('-prj', '--PRJNM', help='Provide the PROJECT Name')
        parser.add_argument('-p', '--PIPNM', help='Provide the PIPELINE Name')
        parser.add_argument('-t', '--TASKNM', help='Provide the TASK Name')
        parser.add_argument('-r', '--RESTART', help="Provide the RESTART values like : \
        ('true', 'True','TRUE','FALSE','false','False')")
        args = parser.parse_args()
        prj_nm =args.PRJNM
        arg_job_pip_nm = args.PIPNM if args.PIPNM is not None else -9999
        arg_job_nm = args.TASKNM if args.TASKNM is not None else -9999
        restart = args.RESTART if args.RESTART is not None else 'False'
        arg_prj_nm = args.PRJNM
        MODE = "NORMAL"
        try:
            with open("paths.json",'r', encoding='utf-8') as jsonfile:
                # reading paths json data started
                paths_data = json.load(jsonfile)
                # reading paths json data completed
        except Exception as error:
            logging.exception("error in reading paths json %s.", str(error))
            raise error
        if args.PRJNM is None:
            logging.error('PROJECT NAME NOT RECEIVED. PROCESS ABORTED')
            sys.exit()
        elif args.PRJNM not in paths_data["projects"]:
            logging.error('PROJECT NOT AVAILABLE. PLEASE CREATE THE PROJECT FIRST')
            sys.exit()
        elif arg_job_nm == -9999 and arg_job_pip_nm == -9999:
            logging.error('either pipeline nor task names NOT RECEIVED. PROCESS ABORTED')
            sys.exit()
        if restart not in ('true', 'True','TRUE','FALSE','false','False'):
            logging.error('input correct value for restart. for values check --help')
            sys.exit()
        elif arg_job_pip_nm == -9999:
            #if pipeline name is not available
            # only task_nm is available in the command
            PATH=paths_data["folder_path"]
            PATH=os.path.expanduser(PATH)
            IS_EXIST = os.path.exists(PATH+paths_data["Program"]+prj_nm+paths_data[
            "pipeline_log_path"])
            if IS_EXIST is True: #if log folder structure exists already
                if (arg_job_pip_nm == -9999) or (arg_job_nm != -9999 and arg_job_pip_nm !=
                 -9999) or (str(arg_job_nm) != "-9999"):
                # entered into task level execution
                    url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+arg_job_nm+JSON
                    response = requests.get(url,timeout=60)
                    if response.status_code != 200:
                        #if json not found in github
                        logging.error("The json file %s DOES NOT exists in the  GITHUB repository \
                        \nPROCESS got ABORTED", arg_job_nm )
                    else:
                        RUN_ID= str(uuid.uuid4())
                        LOGGING_PATH=PATH + paths_data["Program"]+prj_nm+\
                        paths_data["pipeline_log_path"]
                        setup_logger('main_logger', LOGGING_PATH+str(arg_job_nm)+"_TaskLog_"+
                                     RUN_ID+'.log')
                        LOG_FILE_NAME = str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log'
                        main_logger = logging.getLogger('main_logger')
                        main_logger.info("logging operation started.")
                        logging.info("The json file %s exists in the GITHUB repository",arg_job_nm)
                        IS_EXIST = os.path.exists(PATH+DOWNLOAD_FILE)
                        main_logger.info("download.py file downloading operation "
                                         "started.. \ndownload.py file exists: %s", IS_EXIST)
                        if IS_EXIST is False:
                            DOWNLOAD_PATH = PATH+DOWNLOAD_FILE
                            DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,\
                            paths_data["GH_download_file_path"]]
                            subprocess.call(DOWNLOAD_FILE)
                            main_logger.info(DOWNLOAD_LOG_STATEMENT)
                        download = importlib.import_module("download")
                        main_logger.info(MASTER_LOG_STATEMENT)
                        download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                        arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,MODE)
            else: #if log folder structure does not exists already
                if (arg_job_pip_nm == -9999) or (arg_job_nm !=-9999 and arg_job_pip_nm !=-9999) or \
                (str(arg_job_nm) != "-9999"):
                    url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+arg_job_nm+JSON
                    response = requests.get(url,timeout=60)
                    if response.status_code != 200:
                        logging.error("The json file %s DOES NOT exists in the  GITHUB repository\
                        \nPROCESS got ABORTED", arg_job_nm)
                    else: #task
                        RUN_ID= str(uuid.uuid4())
                        LOGGING_PATH=PATH
                        setup_logger('main_logger', PATH+str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log')
                        LOG_FILE_NAME = str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log'
                        main_logger = logging.getLogger('main_logger')
                        main_logger.info("logging operation started...")
                        main_logger.info("The json file %s exists in the  GITHUB repository",\
                                         arg_job_nm)
                        IS_EXIST = os.path.exists(PATH+DOWNLOAD_FILE)
                        main_logger.info("download.py file downloading operation started.."
                        "\n download.py file exists: %s", IS_EXIST)
                        if IS_EXIST is False:
                            DOWNLOAD_PATH = PATH+DOWNLOAD_FILE
                            DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                            paths_data["GH_download_file_path"]]
                            subprocess.call(DOWNLOAD_FILE)
                            main_logger.info(DOWNLOAD_LOG_STATEMENT)
                        download = importlib.import_module("download")
                        main_logger.info(MASTER_LOG_STATEMENT)
                        download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                        arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,MODE)
        elif arg_job_pip_nm != -9999:
            pipeline=arg_job_pip_nm.split(",")
            for i in pipeline:  #-p P_222,P_777,P_666
                #loop for executing multiple pipelines at a time in the command
                arg_job_pip_nm = i
                PATH=paths_data["folder_path"]
                IS_EXIST = os.path.exists(PATH+paths_data["Program"]+prj_nm+paths_data[
                "pipeline_log_path"])
                if IS_EXIST is True:
                    #task
                    if (arg_job_pip_nm == -9999) or (arg_job_nm != -9999 and arg_job_pip_nm !=
                     -9999) or (str(arg_job_nm) != "-9999"):
                        url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+\
                        arg_job_nm+JSON
                        response = requests.get(url,timeout=60)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the GITHUB "
                            "repository \n PROCESS got ABORTED",arg_job_nm)
                        else:
                            LOGGING_PATH=PATH+paths_data["Program"]+prj_nm+\
                            paths_data["pipeline_log_path"]
                            RUN_ID=str(uuid.uuid4())
                            setup_logger('main_logger',LOGGING_PATH+str(arg_job_nm)+
                                         "_TaskLog_"+RUN_ID+'.log')
                            LOG_FILE_NAME = str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log'
                            main_logger = logging.getLogger('main_logger')
                            main_logger.info("logging operation started. \n The json file %s \
                            exists in the  GITHUB repository.", arg_job_nm)
                            IS_EXIST = os.path.exists(PATH+DOWNLOAD_FILE)
                            main_logger.info("download.py file downloading operation started.. \
                            \n download.py file exists: %s", IS_EXIST)
                            if IS_EXIST is False:
                                DOWNLOAD_PATH = PATH+DOWNLOAD_FILE
                                DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                main_logger.info(DOWNLOAD_LOG_STATEMENT)
                            download = importlib.import_module("download")
                            main_logger.info(MASTER_LOG_STATEMENT)
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,MODE)
                    else:
                        #pipeline
                        url=paths_data["projects"][arg_prj_nm]["GH_pipeline_path"]+arg_job_pip_nm+ \
                        JSON
                        response = requests.get(url,timeout=60)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the  GITHUB "
                            "repository \n PROCESS got ABORTED",arg_job_pip_nm)
                            sys.exit()
                        else:

                            LOGGING_PATH= PATH+paths_data["Program"]+prj_nm+\
                            paths_data["pipeline_log_path"]
                            #querying on the audit table for previous status
                            audit_state = execute_query(paths_data, arg_job_pip_nm)
                            if audit_state: #if list is not empty
                                # print("entered if")
                                for run_id,iteration,status in audit_state:
                                    if status != 'COMPLETED' and restart.lower() == 'true':
                                        RUN_ID = run_id
                                        iter_value = iteration+1
                                        ITER_VALUE = str(iter_value)
                                        MODE = "RESTART"
                                        setup_logger('main_logger',LOGGING_PATH+str(arg_job_pip_nm)+
                                        "_PipelineLog_"+RUN_ID+'_'+ITER_VALUE+'.log')
                                    else:
                                        RUN_ID= str(uuid.uuid4())
                                        ITER_VALUE = "1"
                                        setup_logger('main_logger',LOGGING_PATH+str(arg_job_pip_nm)+
                                        "_PipelineLog_"+RUN_ID+'.log')
                            else:
                                # print("entered if else")
                                RUN_ID=str(uuid.uuid4())
                                ITER_VALUE = "1"
                                setup_logger('main_logger',LOGGING_PATH+str(arg_job_pip_nm)+
                                             "_PipelineLog_"+RUN_ID+'.log')
                            LOG_FILE_NAME = str(arg_job_pip_nm)+"_PipelineLog_"+RUN_ID+'.log'
                            main_logger = logging.getLogger('main_logger')
                            main_logger.info("logging operation started.. \n The json file %s \
                            exists in the  GITHUB repository.", arg_job_pip_nm)
                            main_logger.info("RUN_ID:%s",RUN_ID)
                            IS_EXIST = os.path.exists(PATH+DOWNLOAD_FILE)
                            main_logger.info("download.py file downloading operation started.. \n \
                            download.py file exists: %s", IS_EXIST)
                            if IS_EXIST is False:
                                DOWNLOAD_PATH = PATH+DOWNLOAD_FILE
                                DOWNLOAD_FILE=['curl','-o',DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                main_logger.info(DOWNLOAD_LOG_STATEMENT)
                            download = importlib.import_module("download")
                            main_logger.info(MASTER_LOG_STATEMENT)
                            # main_logger.info("MODE:%s", MODE)
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,MODE,ITER_VALUE)
                else: #if log folder structure does not exists already
                    if (arg_job_pip_nm == -9999) or (arg_job_nm !=-9999 and
                    arg_job_pip_nm !=-9999) or (str(arg_job_nm) != "-9999"):
                        #task
                        url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+\
                        arg_job_nm+JSON
                        response = requests.get(url,timeout=60)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the GITHUB "
                            "repository", arg_job_nm)
                            logging.error("PROCESS got ABORTED")
                        else:
                            RUN_ID=str(uuid.uuid4())
                            LOGGING_PATH=PATH
                            setup_logger('main_logger', PATH+str(arg_job_nm)+"_TaskLog_"+
                                         RUN_ID+'.log')
                            LOG_FILE_NAME = str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log'
                            main_logger = logging.getLogger('main_logger')
                            main_logger.info("logging operation started...")
                            main_logger.info("The json file %s exists in the  GITHUB repository.",
                            arg_job_nm)
                            IS_EXIST = os.path.exists(PATH+DOWNLOAD_FILE)
                            main_logger.info("download.py file downloading operation started..")
                            main_logger.info('download.py file exists: %s', IS_EXIST)
                            if IS_EXIST is False:
                                DOWNLOAD_PATH = PATH+DOWNLOAD_FILE
                                DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                main_logger.info(DOWNLOAD_LOG_STATEMENT)
                            download = importlib.import_module("download")
                            main_logger.info(MASTER_LOG_STATEMENT)
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm, RUN_ID,LOGGING_PATH,LOG_FILE_NAME,MODE)
                    else:
                        #pipeline (code needs to be changed here)
                        url=paths_data["projects"][arg_prj_nm]["GH_pipeline_path"]+arg_job_pip_nm+ \
                        JSON
                        response = requests.get(url,timeout=60)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the  GITHUB "
                            "repository5",arg_job_pip_nm)
                            logging.error("PROCESS got ABORTED")
                        else:
                            RUN_ID=str(uuid.uuid4())
                            LOGGING_PATH=PATH
                            setup_logger('main_logger', PATH+str(arg_job_pip_nm)+
                                         "_PipelineLog_"+RUN_ID+'.log')
                            LOG_FILE_NAME = str(arg_job_pip_nm)+"_PipelineLog_"+RUN_ID+'.log'
                            main_logger = logging.getLogger('main_logger')
                            main_logger.info("logging operation started....")
                            main_logger.info("The json file %s exists in the  GITHUB repository.",
                            arg_job_pip_nm)
                            IS_EXIST = os.path.exists(PATH+DOWNLOAD_FILE)
                            main_logger.info("download.py file downloading operation started..")
                            main_logger.info('download.py file exists: %s', IS_EXIST)
                            if IS_EXIST is False:
                                DOWNLOAD_PATH = PATH+DOWNLOAD_FILE
                                DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                main_logger.info(DOWNLOAD_LOG_STATEMENT)
                            download = importlib.import_module("download")
                            main_logger.info(MASTER_LOG_STATEMENT)
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
    except Exception as error:
        main_logger.error("exception occured")
        raise error
    finally:
        sys.exit()

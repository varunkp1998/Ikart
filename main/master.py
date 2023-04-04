""" importing modules """
import logging
import argparse
import sys
import json
import random
import os
import subprocess
from datetime import datetime
import requests

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

if __name__ == "__main__":
    try:
        ######### Setting variables #########################################
        parser = argparse.ArgumentParser()
        parser.add_argument('-prj', '--PRJNM', help='Provide the PROJECT Name')
        parser.add_argument('-p', '--PIPNM', help='Provide the PIPELINE Name')
        parser.add_argument('-t', '--TASKNM', help='Provide the TASK Name')
        parser.add_argument('-r', '--RESTART', help='Provide the RESTART as FALSE or TRUE TO RUN')
        args = parser.parse_args()
        prj_nm =args.PRJNM
        arg_job_pip_nm = args.PIPNM if args.PIPNM is not None else -9999
        arg_job_nm = args.TASKNM if args.TASKNM is not None else -9999
        restart = args.RESTART if args.RESTART is not None else False
        # print("enterd into command")
        arg_prj_nm = args.PRJNM
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
        elif arg_job_pip_nm == -9999:
            #if pipeline name is not available
            PATH=paths_data["folder_path"]
            IS_EXIST = os.path.exists(PATH+paths_data["Program"]+prj_nm+paths_data[
            "pipeline_log_path"])
            if IS_EXIST is True: #if log folder structure exists already
                if (arg_job_pip_nm == -9999) or (arg_job_nm != -9999 and arg_job_pip_nm !=
                 -9999) or (str(arg_job_nm) != "-9999"):
                # entered into task level execution
                    url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+arg_job_nm+'.json'
                    response = requests.get(url)
                    # log1.info(response.status_code)
                    if response.status_code != 200:
                        #if json not found in github
                        logging.error("The json file %s DOES NOT exists in the  GITHUB repository",
                        arg_job_nm)
                        logging.error("PROCESS got ABORTED")
                    else:
                        RUN_ID= str(datetime.now().strftime("%Y%m%d%H%M%S") +'_'+ str(random.
                        randint(1,999)))
                        LOGGING_PATH=PATH + paths_data["Program"]+prj_nm+\
                        paths_data["pipeline_log_path"]
                        setup_logger('log1', LOGGING_PATH+str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log')
                        LOG_FILE_NAME = str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log'
                        log1 = logging.getLogger('log1')
                        log1.info("logging operation started.")
                        logging.info("The json file %s exists in the GITHUB repository",arg_job_nm)
                        IS_EXIST = os.path.exists(PATH+'download.py')
                        log1.info("download.py file downloading operation started..")
                        log1.info('download.py file exists: %s', IS_EXIST)
                        if IS_EXIST is False:
                            # print("entered into false")
                            DOWNLOAD_PATH = PATH+'download.py'
                            DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,\
                            paths_data["GH_download_file_path"]]
                            subprocess.call(DOWNLOAD_FILE)
                            # print("download.py file downloading completed")
                            log1.info("download.py file downloading completed")
                        import download
                        log1.info("Master execution started")
                        download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                        arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
            else: #if log folder structure does not exists already
                if (arg_job_pip_nm == -9999) or (arg_job_nm !=-9999 and arg_job_pip_nm !=-9999) or \
                (str(arg_job_nm) != "-9999"):
                    url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+arg_job_nm+'.json'
                    response = requests.get(url)
                    # log1.info(response.status_code)
                    if response.status_code != 200:
                        logging.error("The json file %s DOES NOT exists in the  GITHUB repository",
                        arg_job_nm)
                        logging.error("PROCESS got ABORTED")
                    else:
                        RUN_ID= str(datetime.now().strftime("%Y%m%d%H%M%S") +'_'+ str(random.
                        randint(1,999)))
                        LOGGING_PATH=PATH
                        setup_logger('log1', PATH+str(arg_job_nm)+"_PipelineLog_"+RUN_ID+'.log')
                        LOG_FILE_NAME = str(arg_job_nm)+"_PipelineLog_"+RUN_ID+'.log'
                        log1 = logging.getLogger('log1')
                        log1.info("logging operation started...")
                        log1.info("The json file %s exists in the  GITHUB repository",arg_job_nm)
                        IS_EXIST = os.path.exists(PATH+'download.py')
                        log1.info("download.py file downloading operation started..")
                        log1.info('download.py file exists: %s', IS_EXIST)
                        if IS_EXIST is False:
                            # print("entered into false")
                            DOWNLOAD_PATH = PATH+'download.py'
                            DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                            paths_data["GH_download_file_path"]]
                            subprocess.call(DOWNLOAD_FILE)
                            # print("download.py file downloading completed")
                            log1.info("download.py file downloading completed")
                        import download
                        log1.info("Master execution started")
                        download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                        arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
        elif arg_job_pip_nm != -9999:
            #if pipeline name exists
            # print("entered into pipeline or task")
            pipeline=arg_job_pip_nm.split(",")
            # print(pipeline)
            for i in pipeline:
                arg_job_pip_nm = i
                # print(arg_job_pip_nm)
                PATH=paths_data["folder_path"]
                IS_EXIST = os.path.exists(PATH+paths_data["Program"]+prj_nm+paths_data[
                "pipeline_log_path"])
                if IS_EXIST is True: #if log folder structure exists already
                    if (arg_job_pip_nm == -9999) or (arg_job_nm != -9999 and arg_job_pip_nm !=
                     -9999) or (str(arg_job_nm) != "-9999"):
                        url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+\
                        arg_job_nm+'.json'
                        response = requests.get(url)
                        # log1.info(response.status_code)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the GITHUB "
                            "repository",arg_job_nm)
                            logging.error("PROCESS got ABORTED")
                        else:
                            # entered into task level execution
                            # logging = logging.getLogger(str(arg_job_nm)+"_TaskLog_"+RUN_ID)
                            LOGGING_PATH=PATH+paths_data["Program"]+prj_nm+\
                            paths_data["pipeline_log_path"]
                            RUN_ID=str(datetime.now().strftime("%Y%m%d%H%M%S") +'_'+ str(random.
                            randint(1,999)))
                            # initiate_logging(str(arg_job_nm)+"_TaskLog_"+RUN_ID,LOGGING_PATH)
                            setup_logger('log1',LOGGING_PATH+str(arg_job_nm)+"_TaskLog_"+RUN_ID+
                            '.log')
                            LOG_FILE_NAME = str(arg_job_nm)+"_TaskLog_"+RUN_ID+'.log'
                            log1 = logging.getLogger('log1')
                            log1.info("logging operation started.")
                            log1.info("The json file %s exists in the  GITHUB repository.",
                            arg_job_nm)
                            IS_EXIST = os.path.exists(PATH+'download.py')
                            log1.info("download.py file downloading operation started..")
                            log1.info('download.py file exists: %s', IS_EXIST)
                            if IS_EXIST is False:
                                # print("entered into false")
                                DOWNLOAD_PATH = PATH+'download.py'
                                DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                # print("download.py file downloading completed")
                                log1.info("download.py file downloading completed")
                            import download
                            log1.info("Master execution started")
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
                    else:
                        url=paths_data["projects"][arg_prj_nm]["GH_pipeline_path"]+arg_job_pip_nm+ \
                        '.json'
                        response = requests.get(url)
                        # log1.info(response.status_code)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the  GITHUB "
                            "repository",arg_job_pip_nm)
                            logging.error("PROCESS got ABORTED")
                            sys.exit()
                        else:
                            # entered into pipeline level execution
                            LOGGING_PATH= PATH+paths_data["Program"]+prj_nm+\
                            paths_data["pipeline_log_path"]
                            RUN_ID=str(datetime.now().strftime("%Y%m%d%H%M%S") +'_'+ str(random.
                            randint(1,999)))
                            setup_logger('log1',LOGGING_PATH+str(arg_job_pip_nm)+"_PipelineLog_"+
                            RUN_ID+'.log')
                            LOG_FILE_NAME = str(arg_job_pip_nm)+"_PipelineLog_"+RUN_ID+'.log'
                            log1 = logging.getLogger('log1')
                            log1.info("logging operation started..")
                            # log1.info("restart is :%s", restart)
                            log1.info("The json file %s exists in the  GITHUB repository.",
                            arg_job_pip_nm)
                            IS_EXIST = os.path.exists(PATH+'download.py')
                            log1.info("download.py file downloading operation started..")
                            log1.info('download.py file exists: %s', IS_EXIST)
                            if IS_EXIST is False:
                                # print("entered into false")
                                DOWNLOAD_PATH = PATH+'download.py'
                                DOWNLOAD_FILE=['curl','-o',DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                # print("download.py file downloading completed")
                                log1.info("download.py file downloading completed")
                            import download
                            log1.info("Master execution started")
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
                else: #if log folder structure does not exists already
                    if (arg_job_pip_nm == -9999) or (arg_job_nm !=-9999 and
                    arg_job_pip_nm !=-9999) or (str(arg_job_nm) != "-9999"):
                        url=paths_data["projects"][arg_prj_nm]["GH_task_jsons_path"]+\
                        arg_job_nm+'.json'
                        response = requests.get(url)
                        # log1.info(response.status_code)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the GITHUB "
                            "repository", arg_job_nm)
                            logging.error("PROCESS got ABORTED")
                        else:
                            RUN_ID=str(datetime.now().strftime("%Y%m%d%H%M%S") +'_'+ str(random.
                            randint(1,999)))
                            LOGGING_PATH=PATH
                            setup_logger('log1', PATH+str(arg_job_nm)+"_PipelineLog_"+RUN_ID+'.log')
                            LOG_FILE_NAME = str(arg_job_nm)+"_PipelineLog_"+RUN_ID+'.log'
                            log1 = logging.getLogger('log1')
                            log1.info("logging operation started...")
                            log1.info("The json file %s exists in the  GITHUB repository.",
                            arg_job_nm)
                            IS_EXIST = os.path.exists(PATH+'download.py')
                            log1.info("download.py file downloading operation started..")
                            log1.info('download.py file exists: %s', IS_EXIST)
                            if IS_EXIST is False:
                                # print("entered into false")
                                DOWNLOAD_PATH = PATH+'download.py'
                                DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                # print("download.py file downloading completed")
                                log1.info("download.py file downloading completed")
                            import download
                            log1.info("Master execution started")
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm, RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
                    else:
                        url=paths_data["projects"][arg_prj_nm]["GH_pipeline_path"]+arg_job_pip_nm+ \
                        '.json'
                        # log1.info(url)
                        response = requests.get(url)
                        # log1.info(response.status_code)
                        if response.status_code != 200:
                            logging.error("The json file %s DOES NOT exists in the  GITHUB "
                            "repository5",arg_job_pip_nm)
                            logging.error("PROCESS got ABORTED")
                        else:
                            RUN_ID=str(datetime.now().strftime("%Y%m%d%H%M%S") +'_'+ str(random.
                            randint(1,999)))
                            LOGGING_PATH=PATH
                            setup_logger('log1', PATH+str(arg_job_pip_nm)+"_PipelineLog_"+RUN_ID+
                            '.log')
                            LOG_FILE_NAME = str(arg_job_pip_nm)+"_PipelineLog_"+RUN_ID+'.log'
                            log1 = logging.getLogger('log1')
                            log1.info("logging operation started....")
                            log1.info("The json file %s exists in the  GITHUB repository.",
                            arg_job_pip_nm)
                            IS_EXIST = os.path.exists(PATH+'download.py')
                            log1.info("download.py file downloading operation started..")
                            log1.info('download.py file exists: %s', IS_EXIST)
                            if IS_EXIST is False:
                                # print("entered into false")
                                DOWNLOAD_PATH = PATH+'download.py'
                                DOWNLOAD_FILE=['curl', '-o', DOWNLOAD_PATH,
                                paths_data["GH_download_file_path"]]
                                subprocess.call(DOWNLOAD_FILE)
                                # print("download.py file downloading completed")
                                log1.info("download.py file downloading completed")
                            import download
                            log1.info("Master execution started")
                            download.execute_pipeline_download(prj_nm,paths_data,arg_job_nm,
                            arg_job_pip_nm,RUN_ID,LOGGING_PATH,LOG_FILE_NAME,restart)
    except Exception as error:
        log1.error("exception occured")
        # print("exception occured")
        # print(error)
        raise error
    finally:
        # print("entered in to final")
        sys.exit()

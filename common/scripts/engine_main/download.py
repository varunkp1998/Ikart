"""script which has all the definitions for master_executor"""
import sys
import subprocess
import logging
import os
import json
import requests

log1 = logging.getLogger('log1')
log2 = logging.getLogger('log2')

def setup_logger(logger_name, log_file, level=logging.INFO):
    """Function to initiate log file"""
    try:
        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter('%(asctime)s | %(name)-10s | %(processName)-12s |\
        %(funcName)-22s | %(levelname)-5s | %(message)s')
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(formatter)
        # file_handler.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        # stream_handler.setLevel(logging.INFO)

        logger.setLevel(level)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        logger.propagate = False
        logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
        logging.getLogger('great_expectations.experimental.datasources').setLevel(logging.WARNING)
        # return logger
    except Exception as ex:
        logging.error('UDF Failed: setup_logger failed')
        raise ex

def create_folder_structure(prj_nm,path: str):
    """Function to create folder stucture in server"""
    log1.info("started creating the folder structure")
    try:
        os.chdir(path)
        if not os.path.exists('Common'):
            os.makedirs('Common')
        os.chdir(path+'Common/')
        if not os.path.exists('Config'):
            os.makedirs('Config')
        if not os.path.exists('Scripts'):
            os.makedirs('Scripts')
        os.chdir(path+'Common/Scripts/')
        if not os.path.exists('engine_main'):
            os.makedirs('engine_main')
        if not os.path.exists('dq_scripts'):
            os.makedirs('dq_scripts')
        if not os.path.exists('ingestion'):
            os.makedirs('ingestion')
        if not os.path.exists('orchestration'):
            os.makedirs('orchestration')
        os.chdir('..')
        os.chdir('..')
        # print('Get current working directory : ', os.getcwd())
        # print(os.path)
        if not os.path.exists('Program'):
            os.makedirs('Program')
        os.chdir(path+'Program/')
        if not os.path.exists(prj_nm):
            os.makedirs(prj_nm)
        os.chdir(path+'Program/'+prj_nm+'/')
        if not os.path.exists('Pipeline'):
            os.makedirs('Pipeline')
        os.chdir(path+'Program/'+prj_nm+'/Pipeline')
        if not os.path.exists('Task'):
            os.makedirs('Task')
        if not os.path.exists('json'):
            os.makedirs('json')
        if not os.path.exists('logs'):
            os.makedirs('logs')
        os.chdir(path+'Program/'+prj_nm+'/Pipeline/Task/')
        if not os.path.exists('archive'):
            os.makedirs('archive')
        if not os.path.exists('audit'):
            os.makedirs('audit')
        if not os.path.exists('json'):
            os.makedirs('json')
        if not os.path.exists('logs'):
            os.makedirs('logs')
        if not os.path.exists('rejected'):
            os.makedirs('rejected')
        if not os.path.exists('source_files'):
            os.makedirs('source_files')
        if not os.path.exists('target_files'):
            os.makedirs('target_files')
        if not os.path.exists('reports'):
            os.makedirs('reports')
        log1.info("completed creating the folder structure")
    except Exception as error:
        log1.exception("error in create_folder_structure %s.", str(error))
        raise error

def download_pipeline_json(prj_nm,pipeline_name:str,paths_data:str):
    """Function to download pipeline JSON from Github to server"""
    try:
        log1.info("downloading pipeline.json from Github started..")
        pipeline_json_path= paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["pipeline_json_path"]
        log1.info("path:%s",pipeline_json_path)
        log1.info("pipeline.json location: %s", pipeline_json_path)
        path_check = pipeline_json_path+pipeline_name+'.json'
        is_exist = os.path.exists(path_check)
        log1.info('pipeline.json file exists: %s', is_exist)
        log1.info("pipeline_path:%s",paths_data["projects"][prj_nm]["GH_pipeline_path"]+
        pipeline_name+'.json')
        if is_exist is False:
            dwn_json = ['curl', '-o',pipeline_json_path+pipeline_name+'.json',
            paths_data["projects"][prj_nm]["GH_pipeline_path"]+pipeline_name+'.json']
            subprocess.call(dwn_json)
            log1.info("downloading pipeline.json:%s from Github completed", pipeline_name)
            log1.info('#####################################################')
    except Exception as error:
        log1.exception("error in download_pipeline_json %s.", str(error))
        raise error

def common_files_downloads(prj_nm,paths_data:str):
    """Function to download engine_code.py,checks_mapping.json,mapping.json,
    definitions_qc,utility from Github to server"""
    try:
        #curl command for downloading the engine_code file
        log1.info("downloading engine_code.py from Github started..")
        engine_path= paths_data["folder_path"]+paths_data["engine_path"]
        log1.info("engine_code.py location: %s", engine_path)
        engine_path_check = engine_path+'engine_code.py'
        is_exist = os.path.exists(engine_path_check)
        log1.info('engine file exists: %s', is_exist)
        if is_exist is False:
            engine = ['curl', '-o', paths_data["folder_path"]+paths_data["engine_path"]+
            'engine_code.py', paths_data["GH_engine_path"]]
            subprocess.call(engine)
            log1.info("downloading engine_code.py from Github completed..")
            log1.info('#####################################################')
        #curl command for downloading the checks mapping file
        log1.info("downloading checks_mapping.json from Github started..")
        checks_mapping_check = paths_data["folder_path"]+paths_data["dq_scripts_path"]+\
        'checks_mapping.json'
        # Check whether the specified path exists or not
        is_exist = os.path.exists(checks_mapping_check)
        log1.info('checks_mapping.json file exists: %s', is_exist)
        if is_exist is False:
            checks_mapping = ['curl', '-o', checks_mapping_check,
            paths_data["GH_checks_mapping_path"]]
            subprocess.call(checks_mapping)
            log1.info("downloading the checks_mapping.json from Github completed")
            log1.info('#####################################################')
        #curl command for downloading the mapping file
        log1.info("downloading mapping.json from Github started..")
        mapping_path_check = paths_data["folder_path"]+paths_data["engine_path"]+'mapping.json'
        # Check whether the specified path exists or not
        is_exist = os.path.exists(mapping_path_check)
        log1.info('mapping.json file exists: %s', is_exist)
        if is_exist is False:
            mapping_file = ['curl', '-o', mapping_path_check,
            paths_data["GH_mapping_path"]]
            subprocess.call(mapping_file)
            log1.info("downloading the mapping.json completed")
            log1.info('#####################################################')
        qc_py = ['curl', '-o', paths_data["folder_path"]+paths_data["dq_scripts_path"]+
        'definitions_qc.py', paths_data["GH_definitions_qc_path"]]
        qc_check_path_check = paths_data["folder_path"]+paths_data["dq_scripts_path"]+\
        'definitions_qc.py'
        log1.info("downloading definitions_qc.py from Github started..")
        is_exist = os.path.exists(qc_check_path_check)
        log1.info('definitions_qc file exists: %s', is_exist)
        if is_exist is False:
            subprocess.call(qc_py)
            log1.info("downloading the definitions_qc.py from github completed")
            log1.info('#####################################################')
        utility_path_check = paths_data["folder_path"]+paths_data["ingestion_path"]+'utility.py'
        log1.info("downloading utility.py from Github started..")
        is_exist = os.path.exists(utility_path_check)
        log1.info('utility file exists: %s', is_exist)
        utility_py = ['curl', '-o', utility_path_check,
        paths_data["GH_utility_path"]]
        if is_exist is False:
            log1.info("downloading the utility.py file form gihub completed")
            subprocess.call(utility_py)
            log1.info('#####################################################')
        download_path_check = paths_data["folder_path"]+paths_data["engine_path"]+'download.py'
        log1.info("downloading download.py from Github started..")
        is_exist = os.path.exists(download_path_check)
        log1.info('download.py file exists: %s', is_exist)
        download_py = ['curl', '-o',download_path_check,paths_data["GH_download_file_path"]]
        if is_exist is False:
            log1.info("downloading the download.py form Github completed")
            subprocess.call(download_py)
            log1.info('#####################################################')
        orchestrate_path_check = paths_data["folder_path"]+paths_data["orchestration_path"]+\
        'orchestrate.py'
        log1.info("downloading orchestrate.py from Github started..")
        is_exist = os.path.exists(orchestrate_path_check)
        log1.info('orchestrate file exists: %s', is_exist)
        orchestrate_py = ['curl', '-o',orchestrate_path_check,paths_data["GH_orchestrate_path"]]
        if is_exist is False:
            log1.info("downloading the orchestrate.py form Github completed")
            subprocess.call(orchestrate_py)
            log1.info('#####################################################')
    except Exception as error:
        log1.exception("error in common_downloads %s.", str(error))
        raise error

def task_json_download(prj_nm,task_name:str, paths_data:str):
    """Function to download Task JSON from Github to server"""
    try:
        log2.info("downloading %s.json from Github started..", task_name)
        url=paths_data["projects"][prj_nm]["GH_task_jsons_path"]+task_name+'.json'
        response = requests.get(url)
        # log1.info(response.status_code)
        if response.status_code == 200:
            url_exists = bool(response.status_code)
            log1.info("The json file exists in the  GITHUB repository.")
        else:
            url_exists = bool(not response.status_code)
            log1.info("The json file DOES NOT exists in the  GITHUB repository")
        if url_exists is False  :
            log1.info("PROCESS got ABORTED")
            sys.exit()
        json_path= paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["task_json_path"]
        log2.info("%s.json location: %s", task_name,json_path)
        path_check = json_path+task_name+'.json'
        is_exist = os.path.exists(path_check)
        log2.info('%s.json exists: %s', task_name,is_exist)
        if is_exist is False:
            dwn_json =  ['curl', '-o', json_path+task_name+'.json',paths_data["projects"][prj_nm]
            ["GH_task_jsons_path"]+task_name+'.json']
            subprocess.call(dwn_json)
            log2.info("downloading %s.json from Github completed",task_name)
            log2.info('#####################################################')
    except Exception as error:
        log2.exception("error in task_downloads %s.", str(error))
        raise error

def download_task_files(prj_nm,task_name:str, paths_data:str):
    """function to download source_connection, target_connection, source.py, target.py
    files from github to server for execution"""
    try:
        log2.info("entered into downloading task related files")
        try:
            with open(r""+paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
            paths_data["task_json_path"]
            +task_name+".json","r",
            encoding='utf-8') as jsonfile:
                config_json = json.load(jsonfile)
        except FileNotFoundError as exc:
            log2.warning("the %s.json path or folder specified does not exists",task_name)
            raise exc
            # sys.exit()
        if (config_json['task']['source']['source_type'])  not in ("csv_read","csvfile_read",
        "excelfile_read","parquetfile_read","jsonfile_read","xmlfile_read","textfile_read"):
            source_conn_file = config_json['task']['source']['connection_name']
            url=paths_data["GH_config_file_path"]+source_conn_file+'.json'
            response = requests.get(url)
            # log1.info(response.status_code)
            if response.status_code == 200:
                url_exists = bool(response.status_code)
                log1.info("The source connection json file exists in the  GITHUB repository.")
            else:
                url_exists = bool(not response.status_code)
                log1.info("The source connection file DOES NOT exists in the GITHUB repository")
            if url_exists is False  :
                log1.info("PROCESS got ABORTED")
                sys.exit()
            source_path_check = paths_data["folder_path"]+paths_data["config_path"]+\
            source_conn_file+'.json'
            is_exist = os.path.exists(source_path_check)
            log2.info("downloading the source connection file: %s.json operation started",
            source_conn_file)
            log2.info('source_conn_file.json exists: %s', is_exist)
            if is_exist is False:
                src_json = ['curl', '-o',paths_data["folder_path"]+paths_data["config_path"]+\
                source_conn_file+'.json',
                paths_data["GH_config_file_path"]+source_conn_file+'.json']
                subprocess.call(src_json)
                log2.info("downloading source connection file: %s.json from Github completed..")
                log2.info('#####################################################')
        if (config_json['task']['target']['target_type'])  not in ("csv_write","csvfile_write",
        "parquetfile_write","excelfile_write","jsonfile_write","xmlfile_write","textfile_write"):
            #curl command for downloading the target connection JSON
            target_conn_file = config_json['task']['target']['connection_name']
            url=paths_data["GH_config_file_path"]+target_conn_file+'.json'
            response = requests.get(url)
            # log1.info(response.status_code)
            if response.status_code == 200:
                url_exists = bool(response.status_code)
                log1.info("The target connection json file exists in the  GITHUB repository.")
            else:
                url_exists = bool(not response.status_code)
                log1.info("The target connection file DOES NOT exists in the GITHUB repository")
            if url_exists is False  :
                log1.info("PROCESS got ABORTED")
                sys.exit()
            target_path_check = paths_data["folder_path"]+paths_data["config_path"]+\
            target_conn_file+'.json'
            # Check whether the specified path exists or not
            is_exist = os.path.exists(target_path_check)
            log2.info("downloading the target connection file: %s.json "
            "operation started", target_conn_file)
            log2.info('target_conn_file.json file exists: %s', is_exist)
            if is_exist is False:
                trgt_json = ['curl', '-o', paths_data["folder_path"]+paths_data["config_path"]+
                target_conn_file+'.json',paths_data["GH_config_file_path"]+target_conn_file+'.json']
                subprocess.call(trgt_json)
                log2.info("downloading target connection %s.json from Github completed",
                target_conn_file)
                log2.info('#####################################################')
        source_type = config_json['task']['source']['source_type']
        target_type = config_json['task']['target']['target_type']
        with open(r""+paths_data["folder_path"]+paths_data["engine_path"]+'mapping.json',"r",
        encoding='utf-8') as mapjson:
            config_new_json = json.load(mapjson)
        source_file_name=config_new_json["mapping"][source_type]
        target_file_name= config_new_json["mapping"][target_type]
        src_py = ['curl', '-o',paths_data["folder_path"]+paths_data["ingestion_path"]+
        source_file_name, paths_data["GH_source_ingestion_path"]+source_file_name]
        trgt_py = ['curl', '-o', paths_data["folder_path"]+paths_data["ingestion_path"]+
        target_file_name, paths_data["GH_target_ingestion_path"]+target_file_name]
        src_py_path_check = paths_data["folder_path"]+paths_data["ingestion_path"]+source_file_name
        is_exist = os.path.exists(src_py_path_check)
        log2.info('source read file exists: %s', is_exist)
        if is_exist is False:
            log2.info("downloading the  source read file: %s", source_file_name)
            subprocess.call(src_py)
            log2.info('#####################################################')
        trg_py_path_check = paths_data["folder_path"]+paths_data["ingestion_path"]+target_file_name
        is_exist = os.path.exists(trg_py_path_check)
        log2.info('target write file exists: %s', is_exist)
        if is_exist is False:
            log2.info("downloading the  target write file: %s", target_file_name)
            subprocess.call(trgt_py)
            log2.info('#####################################################')
    except Exception as error:
        log2.exception("error in download_task_files %s.", str(error))
        raise error

def execute_pipeline_download(prj_nm,paths_data:str,task_name:str,pipeline_name:str,run_id:str,
    log_file_path,log_file_name):
    """executes pipeline flow"""
    try:
        # initiate_logging('log',r"D:\\")
        log1.info("calling the create_folder_structure function")
        create_folder_structure(prj_nm,paths_data["folder_path"],)
        if task_name == -9999 :
            download_pipeline_json(prj_nm,pipeline_name,paths_data)
        log1.info("calling the common_files_downloads function")
        common_files_downloads(prj_nm,paths_data)
        orchestration_script=paths_data["folder_path"]+paths_data["orchestration_path"]
        # logging.info(dq_scripts_path)
        sys.path.insert(0, orchestration_script)
        import orchestrate
        log1.info("calling the orchestrate_calling function")
        orchestrate.orchestrate_calling(prj_nm,paths_data,task_name,pipeline_name,run_id,
        log_file_path,log_file_name)
        # return begin
    except Exception as error:
        log1.exception("error in execute_pipeline_download %s.", str(error))
        raise error

def execute_engine(prj_nm,task_name:str,paths_data:str,run_id:str,pip_nm:str):
    """engine execution code"""
    try:
        logging_path= paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["task_log_path"]
        # initiate_logging(task_name,logging_path)
        # log=logging.getLOGGER(task_name)
        # initiate_logging(task_name+"_TaskLog_"+run_id,logging_path)
        setup_logger('log2', logging_path+task_name+"_TaskLog_"+run_id+'.log')
        log2.info("entered into execute_engine")
        new_path = paths_data["folder_path"] +paths_data["engine_path"]
        # print(new_path)
        #initiate_logging(task_name+"_TaskLog_"+run_id,logging_path)
        log2.info("calling the task_json_download function")
        task_json_download(prj_nm,task_name,paths_data)
        print("calling the download_task_files function")
        download_task_files(prj_nm,task_name,paths_data)
        sys.path.insert(0, new_path)
        #  os.chdir(new_path)
        import engine_code
        log2.info('#####################################################')
        log2.info("calling the engine_main")
        engine_code.engine_main(prj_nm,task_name,paths_data,run_id,pip_nm)
        log2.info('#####################################################')
    except Exception as error:
        log2.exception("error in executing engine %s.", str(error))
        raise error

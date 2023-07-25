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
from github import Github
from github import Auth
from pathlib import Path
from dotenv import load_dotenv 

JSON = ".json"

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

def decrypt(data):
    '''function to decrypt the data'''
    KEY = b'8ookgvdIiH2YOgBnAju6Nmxtp14fn8d3'
    IV = b'rBEssDfxofOveRxR'
    aesgcm = AESGCM(KEY)
    decrypted = aesgcm.decrypt(IV, bytes.fromhex(data), None)
    return decrypted.decode('utf-8')


def get_config_section(config_path:str) -> dict:
    """reads the connection file and returns connection details as dict for
       connection name you pass it through the json
    """
    try:
        with open(config_path,'r', encoding='utf-8') as json_file:

            json_data = json.load(json_file)
            return dict(json_data["connection_details"].items())
    except Exception as err:
        logging.exception("get_config_section() is %s.", str(err))
        raise err

def execute_query(config_paths, pip_nm):
    """Gets audit status from the audit table for the given pipeline."""
    result = None  # Initialize result variable outside the try block
    try:
        logging.info("task name from API: %s", pip_nm)
        url = f"{config_paths['audit_api_url']}/executeTaskOrPiplineQuery/{pip_nm}"
        logging.info("URL from API: %s", url)    
        response = requests.get(url, timeout=100) 
        if response.status_code == 200:
            logging.info("Task name from API response: %s", response.json())
            result = response.json()
        else:
            logging.info("Request failed with status code: %s", response.status_code)
        return result
    except Exception as err:
        logging.exception("execute_query() error: %s", str(err))
        raise err


def get_file_in_gitrepo(repo, path, file_or_dir, branch):
    '''to get the files from git repo using pygithub'''
    try:
        auth_token = os.getenv("AUTH")
        auth = Auth.Token(auth_token)
        g = Github(auth=auth)
        repo=g.get_repo(repo)
        content_list = [file.name for file in repo.get_contents(path, ref=branch) if file.type == file_or_dir]
        return content_list
    except Exception as err:
        logging.exception("get_file_in_gitrepo() error: %s", str(err))
        raise err


def log_creation(logging_path, task_name, pipeline_name, run_id):
    """function to create run id and log"""
    try:
        main_logger.info("logging operation started.")
        if task_name:
            log_file_name = str(task_name) + "_taskLog_" + run_id + '.log'
        elif pipeline_name:
            log_file_name = str(pipeline_name) + "_pipelineLog_" + run_id + '.log'
        setup_logger('main_logger', str(logging_path) + str(log_file_name))
        logging.info("The json file %s exists in the GITHUB repository", task_name
                     or pipeline_name)
    except Exception as err:
        logging.info("error in log_creation %s", str(err))
        raise err

def downlaod_file_from_git(repo,file_path,save_dir,branch):
    '''function to download the file from git'''
    auth_token = os.getenv("AUTH")
    auth = Auth.Token(auth_token)
    g = Github(auth=auth)
    repo = g.get_repo(repo)
    file_contents = repo.get_contents(file_path,ref=branch).decoded_content
    file_name = Path(file_path).name
    save_path = Path(save_dir).joinpath(file_name)
    print(save_path)
    with open(save_path, 'wb') as file:
        file.write(file_contents)
    return file

def get_download_from_git(configs_path,repo,task_name,run_id,home_path,branch):
    '''function to get the download.py from git'''
    try:
        log_file_name = str(task_name)+"_taskLog_"+run_id+'.log'
        file_path = configs_path['GH_download_file_path']
        save_dir = home_path
        downlaod_file_from_git(repo,file_path,save_dir,branch)
        main_logger.info("download.py file downloading completed")
    except Exception as err:
        logging.info("error in get_download_from_git %s", str(err))
        raise err


if __name__ == "__main__":
    try:
        MODE = "NORMAL"
        parser = argparse.ArgumentParser(description="IngKart framework orchestration master execution cli.")
        parser.add_argument('-p', dest='project_name',required=True, type=str ,help='Provide the PROJECT Name')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-t', dest='task_name', type=str ,help='Provide the Task Name')
        group.add_argument('-b', dest='pipeline_name', type=str ,help='Provide the Pipeline(Batch) Name')
        parser.add_argument('-gitbranch', dest='git_branch', type=str ,default='main', help="Git Envirnoment to use.")
        parser.add_argument('-r', dest='restart',default=False, type=bool ,help="Provide the Restart values like : \
        ('true', 'True', 'TRUE', 'FALSE','false', 'False')")
        args = parser.parse_args()
        project_name = args.project_name
        git_branch = args.git_branch
        pipeline_name = args.pipeline_name
        task_name = args.task_name
        restart = args.restart
        run_id= str(uuid.uuid4())
        try:
            with open("paths.json",'r', encoding='utf-8') as jsonfile:
                # reading paths json data started
                config_paths = json.load(jsonfile)
                # reading paths json data completed
        except Exception as error:
            logging.exception("error in reading paths json %s.", str(error))
            raise error
        load_dotenv(config_paths["folder_path"]+"env") 
        github_repo_name=config_paths["github_repo_name"]
        repo_path=config_paths["repo_path"]
        if  project_name not in get_file_in_gitrepo(repo=github_repo_name, path = repo_path,file_or_dir='dir', branch=git_branch):
            print('Project not available. create a project and restart...')
            sys.exit()
        elif pipeline_name and (pipeline_name+JSON) not in get_file_in_gitrepo(repo=github_repo_name, path= f'{repo_path}/{project_name}/pipeline',file_or_dir='file',  branch=git_branch):
            print('Pipeline not available. check pipeline job and restart...')
            sys.exit()
        elif task_name and (task_name+JSON) not in get_file_in_gitrepo(repo=github_repo_name, path=f'{repo_path}/{project_name}/task',file_or_dir='file',  branch=git_branch):
            print('Task not available. check task job and restart...')
            sys.exit()
        home_path=Path(config_paths["folder_path"]).expanduser()
        log_file_name = str(task_name)+"_taskLog_"+run_id+'.log'
        main_logger = logging.getLogger('main_logger')

        # check if the task log path exists if not create log folder
        if not Path(str(home_path)+config_paths["Program"] + project_name +config_paths['pipeline_log_path']):
            dir_path = Path(home_path+config_paths["Program"] + project_name +config_paths['pipeline_log_path'])
            dir_path.mkdir(parents=True, exist_ok=True)
        pipeline_log_path = str(home_path)+"/"+config_paths["Program"] + project_name +config_paths['pipeline_log_path']
        

        # check if the task log path exists if not create log folder
        if not Path(str(home_path)+config_paths["Program"] + project_name +config_paths['task_log_path']):
            dir_path = Path(home_path+config_paths["Program"] + project_name +config_paths['task_log_path'])
            dir_path.mkdir(parents=True, exist_ok=True)
        task_log_path =str(home_path)+"/"+config_paths["Program"] + project_name +config_paths['task_log_path']

        # Download the download.py from git.
        print("download.py file downloading operation started...")
        get_download_from_git(config_paths,github_repo_name,task_name,run_id,str(home_path),git_branch)
        print("download.py file downloading operation Completed.")
       
        if task_name:
            log_creation(task_log_path, task_name, pipeline_name, run_id)
            download = importlib.import_module("download")
            main_logger.info("Master execution started")
            download.execute_pipeline_download(project_name, config_paths, task_name, pipeline_name, run_id, task_log_path, log_file_name, MODE) 
        elif pipeline_name:
            audit_state = execute_query(config_paths,pipeline_name)
            if audit_state:
                for audit_data in audit_state:
                    if audit_data['audit_value'] != 'COMPLETED':
                        run_id = audit_data['run_id']
                        ITER_VALUE = str(int(audit_data['iteration']) + 1)
                        MODE = "RESTART"
            log_creation(pipeline_log_path, task_name, pipeline_name, run_id)
            download = importlib.import_module("download")
            main_logger.info("Master execution started")
            download.execute_pipeline_download(project_name, config_paths, task_name, pipeline_name, run_id, task_log_path, log_file_name, MODE) 
    except Exception as error:
        main_logger.error("exception occured")
        raise error
    finally:
        sys.exit()

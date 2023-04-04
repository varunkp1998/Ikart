""" script for reading data from xml"""
import logging
import sys
import os
import pandas as pd

log2 = logging.getLogger('log2')

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

def read(prj_nm,json_data : dict,task_id,run_id,paths_data,file_path) -> bool:
    """ function for reading data from json  """
    try:
        log2.info("json  reading started")
        source_file_path = json_data["task"]["source"]["source_file_path"]+\
            json_data["task"]["source"]["source_file_name"]
        log2.info("list of files which were read")
        log2.info(source_file_path)
        is_exists = os.path.exists(source_file_path)
        engine_code_path = paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, engine_code_path)
        from engine_code import audit
        audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
        paths_data["audit_path"]+task_id+'_audit_'+run_id+'.json'
        # if pip_nm == "-9999":
        #     audit_json_path = paths_data["folder_path"] +paths_data["audit_path"]+task_id+\
        #         '_audit_'+run_id+'.json'
        # else:
        #     audit_json_path = paths_data["folder_path"] +paths_data["audit_path"]+pip_nm+\
        #         '_audit_'+run_id+'.json'
        if is_exists is False:
            log2.error("'%s' SOURCE FILE not found in the location",
            json_data["task"]["source"]["source_file_name"])
            write_to_txt(task_id,'FAILED',file_path)
            audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
            sys.exit()
        else:
            log2.info("entered into else")
            datafram = pd.read_json(json_data["task"]["source"]["source_file_path"]+\
                json_data["task"]["source"]["source_file_name"],
                orient ='index',encoding = json_data["task"]["source"]["encoding"], nrows = None)
        # df=pd.DataFrame(index=file.index).reset_index().astype(str)
        # frames = [df, file]
        # datafram = pd.concat(frames)
            datafram.columns = datafram.columns.astype(str)
        # print(file)
        yield datafram
        return True
    except Exception as error:
        write_to_txt(task_id,'FAILED',file_path)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        log2.exception("reading json() is %s", str(error))
        raise error

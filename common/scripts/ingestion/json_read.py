""" script for reading data from xml"""
import logging
import sys
import os
import pandas as pd

log2 = logging.getLogger('log2')

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

def read(prj_nm,json_data : dict,task_id,run_id,pip_nm,paths_data) -> bool:
    """ function for reading data from json  """
    try:
        log2.info("json  reading started")
        file_path = json_data["task"]["source"]["source_file_path"]+\
            json_data["task"]["source"]["source_file_name"]
        log2.info("list of files which were read")
        log2.info(file_path)
        is_exists = os.path.exists(file_path)
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
            status1= 'FAILED'
            write_to_txt(prj_nm,task_id,status1,run_id,paths_data)
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
        log2.exception("reading json() is %s", str(error))
        raise error

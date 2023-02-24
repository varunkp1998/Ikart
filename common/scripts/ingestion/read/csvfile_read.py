""" script for reading data from csv"""
import sys
import glob
import logging
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


def read(prj_nm,json_data: dict,task_id,run_id,pip_nm,paths_data, delimiter = ",",skip_header = 0,skip_footer= 0, quotechar = '"', escapechar = None) -> bool:
    """ function for reading data from csv"""
    try:
        log2.info("reading csv initiated...")
        path = json_data["task"]["source"]["source_file_path"]+\
        json_data["task"]["source"]["source_file_name"]
        # function for reading files present in a folder with different csv formats
        all_files = [f for f_ in [glob.glob(e) for e in (f'{path}*.zip',f'{path}*.csv',
        f'{path}*.csv.zip', f'{path}*.gz', f'{path}*.bz2',f'{path}*.txt') ] for f in f_]
        log2.info("list of files which were read")
        log2.info(all_files)
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
        if all_files == []:
            log2.error("'%s' SOURCE FILE not found in the location",
            json_data["task"]["source"]["source_file_name"])
            status1= 'FAILED'
            write_to_txt(prj_nm,task_id,status1,run_id,paths_data)
            audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
            sys.exit()
        else:
            default_delimiter = delimiter if json_data["task"]["source"]["delimiter"]==" " else\
            json_data["task"]["source"]["delimiter"]
            default_skip_header=skip_header if json_data["task"]["source"]["skip_header"]==" " else\
            json_data["task"]["source"]["skip_header"]
            default_skip_footer=skip_footer if json_data["task"]["source"]["skip_footer"]==" " else\
            json_data["task"]["source"]["skip_footer"]
            default_quotechar = quotechar if json_data["task"]["source"]["quote_char"]==" " else\
            json_data["task"]["source"]["quote_char"]
            default_escapechar = escapechar if json_data["task"]["source"]["escape_char"]==" " else\
            json_data["task"]["source"]["escape_char"]
            default_select_cols = None if json_data["task"]["source"]["select_columns"]==" " else\
            list(json_data["task"]["source"]["select_columns"].split(","))
            default_alias_cols = None if json_data["task"]["source"]["alias_columns"]==" " else\
            list(json_data["task"]["source"]["alias_columns"].split(","))
            default_encoding = "utf-8" if json_data["task"]["source"]["encoding"]==" " else\
            json_data["task"]["source"]["encoding"]
            # print(default_alias_cols)
            default_header ='infer' if json_data["task"]["source"]["alias_columns"] == " " else None
            count = 0
            # df = pd.DataFrame()
            for file in all_files:
                count +=1
                data = pd.read_csv(filepath_or_buffer = file,encoding=default_encoding,
                low_memory=False)
                # print(type(data))
                row_count = data.shape[0]-default_skip_header-default_skip_footer
                log2.info("the number of records in source file is:%s", row_count)
                count1 = 0
                # print(row_count)
                datafram = pd.read_csv(filepath_or_buffer = file, names = default_alias_cols,
                header = default_header,sep = default_delimiter,usecols = default_select_cols,
                skiprows = default_skip_header,nrows = row_count,quotechar = default_quotechar,
                escapechar = default_escapechar,encoding = default_encoding)
            count1 = 1 + count1
            log2.info('%s iteration' , str(count1))
            yield datafram
    except Exception as error:
        log2.exception("reading_csv() is %s", str(error))
        raise error
        
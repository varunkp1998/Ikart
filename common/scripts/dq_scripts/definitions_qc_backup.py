""" importing modules """
from datetime import datetime
import multiprocessing
from multiprocessing.pool import ThreadPool
import logging
import os
import sys
import base64
import json
from os import path
import glob
import great_expectations as ge
import sqlalchemy
import numpy as np
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

log2 = logging.getLogger('log2')

def encrypt(data):
    '''function to Encrypting the data'''
    crypto_key= '8ookgvdIiH2YOgBnAju6Nmxtp14fn8d3'
    crypto_iv= 'rBEssDfxofOveRxR'
    block_size=16
    key = bytes(crypto_key, 'utf-8')
    key2 = bytes(crypto_iv, 'utf-8')
    aes = AES.new(key, AES.MODE_CBC, key2)
    encrypted = aes.encrypt(pad(data.encode(), block_size))
    # Make sure to strip "=" padding since urlsafe-base64 node module strips "=" as well
    return base64.urlsafe_b64encode(encrypted).decode("utf-8").rstrip("=")

def write_to_txt1(task_id,status,file_path):
    """Generates a text file with statuses for orchestration"""
    try:
        is_exist = os.path.exists(file_path)
        if is_exist is True:
            # log2.info("txt getting called")
            data_fram =  pd.read_csv(file_path, sep='\t')
            data_fram.loc[data_fram['task_name']==task_id, 'Job_Status'] = status
            data_fram.to_csv(file_path ,mode='w', sep='\t',index = False, header=True)
    except Exception as error:
        log2.exception("write_to_txt1: %s.", str(error))
        raise error

def audit(json_file_path,json_data, task_name,run_id,status,value):
    """ create audit json"""
    try:
        if path.isfile(json_file_path) is False:
            log2.info('audit started')
            # Data to be written
            audit_data = [{
                "project_id": json_data["project_id"],
                "task/pipeline_name": task_name,
                "run_id": run_id,
                "iteration": "1",
                "audit_type": status,
                "audit_value": value,
                "process_dttm" : datetime.now()
            }]
            # Serializing json
            json_object = json.dumps(audit_data, indent=4, default=str)
            # Writing to sample.json
            with open(json_file_path, "w", encoding='utf-8') as outfile:
                outfile.write(json_object)
            # outfile.close()
            log2.info("audit json file created and audit done")
        else:
            with open(json_file_path, "r+", encoding='utf-8') as audit1:
                audit_data = json.load(audit1)
                audit_data.append(
                    {
                    "project_id": json_data["project_id"],
                    "task/pipeline_name": task_name,
                    "run_id": run_id,
                    "iteration": "1",
                    "audit_type": status,
                    "audit_value": value,
                    "process_dttm" : datetime.now()
                    })
                audit1.seek(0)
                json.dump(audit_data, audit1, indent=4, default=str)
                # audit1.close()
                log2.info('Audit of an event has been made')
    except Exception as error:
        log2.exception("error in auditing json %s.", str(error))
        raise error


def run_checks_in_parallel(index, cols, control_table_df, checks_mapping_df, ge_df,
main_json_file,task_id,run_id, file_path,audit_json_path):
    """Running all the checks specified in control table in parallel"""
    try:
        output_df = pd.DataFrame(
            columns=cols + ['unexpected_index_list', 'threshold_voilated_flag', 'run_flag',
            'result', 'output_reference', 'start_time', 'end_time', 'good_records_file',
             'bad_records_file', 'good_records_count', 'bad_records_count'])
        output_df.at[index,'threshold_voilated_flag'] = 'N'
        output_df.at[index, 'run_flag'] = 'N'
        log2.info('QC for %s check started', control_table_df.at[index, "check"])
        for col in cols:
            output_df.at[index, col] = control_table_df.at[index, col]
        if control_table_df.at[index, 'active'] == 'Y':
            output_df.at[index, 'run_flag'] = 'Y'
            output_df.at[index, 'start_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            checks_nm = control_table_df.at[index, 'check']
            inputs_required = list(control_table_df.at[index, 'parameters'].values())
            #This section covers all the great expectations QA checks
            func_name = 'expect_' + checks_nm
            ge_flag = checks_mapping_df[
                checks_mapping_df['func_name'] == func_name]['type'].item()
            default_parameters_dtypes = checks_mapping_df[
                checks_mapping_df['func_name'] == func_name]['parameters'].item().split('|')
            if ge_flag=='GE':
                ge_df_func = "ge_df." + func_name + '('
                for i, ele in enumerate(default_parameters_dtypes):
                    if ele == 'string':
                        ge_df_func = ge_df_func + "'{" + f"{i}" + "}',"
                    else:
                        ge_df_func = ge_df_func + "{" + f"{i}" + "},"
                ge_df_func = ge_df_func + "result_format = 'COMPLETE')"
                ge_df_func = ge_df_func.format(*inputs_required)
                log2.info('GE function generated - %s', ge_df_func)
                res = eval(ge_df_func)
                output_df.at[index, 'result'] = 'PASS' if res['success'] is True else 'FAIL'
                log2.info('QC for %s check has been %s', control_table_df.at[
                    index,"check"], output_df.at[index, "result"])
                if not res['success'] and control_table_df.at[index, 'ignore_bad_records'] == 'Y':
                    output_df.at[index, 'output_reference'] = res['result']
                    if 'unexpected_index_list' in res['result']:
                        output_df.at[
                            index, 'unexpected_index_list'] = res['result']['unexpected_index_list']
                        if control_table_df.at[
                            index, 'threshold_bad_records'] < res['result']['unexpected_percent']:
                            output_df.at[index,'threshold_voilated_flag'] = 'Y'
                    else:
                        output_df.at[index, 'unexpected_index_list'] = []
                if isinstance(output_df.at[index, 'unexpected_index_list'], float):
                    output_df.at[index, 'unexpected_index_list'] = []
                output_df.at[index, 'good_records_count']=ge_df.shape[0] - len(output_df.at[
                    index, 'unexpected_index_list'])
                output_df.at[index, 'bad_records_count']=ge_df.shape[0] - output_df.at[
                    index, 'good_records_count']
                output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            #Custom Checks Starts from here onwards
            else:
                checks_name = control_table_df.at[index, "check"]
                src_file_name = main_json_file['task']['source']['source_file_name']
                if checks_name in ('reconciliation', 'column_count_comparison'):
                    s_path = main_json_file['task']['source']['source_file_path']+ \
                    src_file_name+ '.csv'
                    src_df = pd.read_csv(s_path)
                    tgt_df = ge_df
                    if checks_name == 'reconciliation':
                        if inputs_required[0] == 'sum':
                            src_sum = src_df[inputs_required[1]].sum()
                            tgt_sum = tgt_df[inputs_required[2]].sum()
                            sum_res = 'PASS' if src_sum == tgt_sum else 'FAIL'
                            output_df.at[index, 'result'] = sum_res
                            log2.info('QC for reconciliation_sum check %s|%s|%s',
                            src_sum, tgt_sum, sum_res)
                            if sum_res == 'FAIL':
                                output_df.at[index, 'output_reference'] = \
                                f'Sum of the source column-{ inputs_required[1]} is {src_sum},\
                                Sum of the target column {inputs_required[2]} is {tgt_sum}'
                            output_df.at[index, 'end_time'] = datetime.now().strftime(
                                "%d/%m/%Y %H:%M:%S")
                        elif inputs_required[0] == 'avg':
                            src_avg = src_df[inputs_required[1]].mean()
                            tgt_avg = tgt_df[inputs_required[2]].mean()
                            avg_res = 'PASS' if src_avg == tgt_avg else 'FAIL'
                            output_df.at[index, 'result'] = avg_res
                            log2.info('QC for reconciliation_avg check %s|%s|%s',src_avg,
                            tgt_avg, avg_res)
                            if avg_res == 'FAIL':
                                output_df.at[index, 'output_reference'] = \
                                f'Avg of the source column-{inputs_required[1]} is {src_avg},\
                                Avg of the target column {inputs_required[2]} is {tgt_avg}'
                            output_df.at[index, 'end_time'] = datetime.now().strftime(
                                "%d/%m/%Y %H:%M:%S")
                        elif inputs_required[0] == 'min':
                            src_min = src_df[inputs_required[1]].min()
                            tgt_min = tgt_df[inputs_required[2]].min()
                            min_res = 'PASS' if src_min == tgt_min else 'FAIL'
                            output_df.at[index, 'result'] = min_res
                            log2.info('QC for reconciliation_min check %s|%s|%s',src_min,
                            tgt_min, min_res)
                            if min_res == 'FAIL':
                                output_df.at[index, 'output_reference'] = \
                                f'Min of the source column-{inputs_required[1]} is {src_min},\
                                Min of the target column {inputs_required[2]} is {tgt_min}'
                            output_df.at[index, 'end_time'] = datetime.now().strftime(
                                "%d/%m/%Y %H:%M:%S")
                        elif inputs_required[0] == 'max':
                            src_max = src_df[inputs_required[1]].max()
                            tgt_max = tgt_df[inputs_required[2]].max()
                            max_res = 'PASS' if src_max == tgt_max else 'FAIL'
                            output_df.at[index, 'result'] = max_res
                            log2.info('QC for reconciliation_max check %s|%s|%s',src_max,
                            tgt_max, max_res)
                            if max_res == 'FAIL':
                                output_df.at[index, 'output_reference'] = \
                                f'Max of the source column-{inputs_required[1]} is {src_max},\
                                Max of the target column {inputs_required[2]} is {tgt_max}'
                            output_df.at[index, 'end_time'] = datetime.now().strftime(
                                "%d/%m/%Y %H:%M:%S")
                        elif inputs_required[0] == 'count':
                            src_count = src_df[inputs_required[1]].count()
                            tgt_count = tgt_df[inputs_required[2]].count()
                            count_res = 'PASS' if src_count == tgt_count else 'FAIL'
                            output_df.at[index, 'result'] = count_res
                            log2.info('QC for reconciliation_count check %s|%s|%s',src_count,
                            tgt_count, count_res)
                            if count_res == 'FAIL':
                                output_df.at[index, 'output_reference'] = \
                                f'Count of the source column-{inputs_required[1]} is {src_count},\
                                Count of the target column {inputs_required[2]} is {tgt_count}'
                            output_df.at[index, 'end_time'] = datetime.now().strftime(
                                "%d/%m/%Y %H:%M:%S")
                    #column count comparison
                    elif checks_name == 'column_count_comparison':
                        if inputs_required[0] == '':
                            src_df.columns = map(str.upper, src_df.columns)
                            tgt_df.columns = map(str.upper, tgt_df.columns)
                            if main_json_file['task']['target']['audit_columns'] == 'active':
                                tgt_df = tgt_df.drop(columns=['CRTD_BY','CRTD_DTTM',
                                'UPDT_BY','UPDT_DTTM'])
                            else:
                                pass
                            src_shape = src_df.shape[1]
                            tgt_shape = tgt_df.shape[1]
                            result = 'PASS' if src_df.shape[1] == tgt_df.shape[1] else 'FAIL'
                            output_df.at[index, 'result'] = result
                            log2.info('QC for %s check %s|%s|%s', checks_name,src_shape,
                            tgt_shape, result)
                            if result == 'FAIL':
                                output_df.at[index, 'output_reference'] = \
                                f'source column count is-{src_shape} and\
                                target column count is {tgt_shape}'
                    output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                #Schema Comparision
                elif checks_name == 'schema_comparison':
                    spath = main_json_file['task']['source']['source_file_path']+ \
                    src_file_name+'.csv'
                    src_df = pd.read_csv(spath)
                    tgt_df = ge_df
                    src_df.columns = map(str.upper, src_df.columns)
                    tgt_df.columns = map(str.upper, tgt_df.columns)
                    if main_json_file['task']['target']['audit_columns'] == 'active':
                        tgt_df = tgt_df.drop(columns=['CRTD_BY','CRTD_DTTM','UPDT_BY','UPDT_DTTM'])
                    else:
                        pass
                    src_columns = src_df.columns
                    tgt_columns = tgt_df.columns
                    src_dtypes = src_df.dtypes
                    tgt_dtypes = tgt_df.dtypes
                    dtypes1 = [str(i) for i in src_dtypes.tolist()]
                    res1 = [i+'|'+j for i,j in zip(src_columns,dtypes1)]
                    dtypes2 = [str(i) for i in tgt_dtypes.tolist()]
                    res2 = [i+'|'+j for i,j in zip(tgt_columns,dtypes2)]
                    if res1 == res2:
                        output_df.at[index, 'result'] = "PASS"
                    else:
                        output_df.at[index, 'result'] = "FAIL"
                        source_set = set(res1)
                        target_set = set(res2)
                        diff_cols = [x for x in source_set if x not in target_set]
                        if not diff_cols:
                            diff_cols = [x for x in target_set if x not in source_set]
                        diff_cols = ','.join(diff_cols)
                        output_df.at[index, 'output_reference'] = \
                        f'These columns {diff_cols} are creating a schema difference'
                    log2.info('QC for %s check has been %s', checks_name,
                        output_df.at[index, "result"])
                    output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                #one-to-one and multi-to-one mapping
                elif checks_name in {'one_to_one_mapping', 'multi_to_one_mapping'}:
                #one-to-one mapping
                    if checks_name == 'one_to_one_mapping':
                        source_df = ge_df
                        source_df = source_df.drop_duplicates(subset=[inputs_required[0],
                        inputs_required[1]], keep='first').reset_index(drop = True)
                        first = source_df.groupby(inputs_required[0])[
                            inputs_required[1]].count().max()
                        second = source_df.groupby(inputs_required[1])[
                            inputs_required[0]].count().max()
                        if first + second == 2:
                            output_df.at[index,'result'] = 'PASS'
                            log2.info('QC for %s check %s|%s|%s', checks_name,
                            inputs_required[0], inputs_required[1], output_df.at[index, "result"])
                        else:
                            output_df.at[index,'result'] = 'FAIL'
                            or_df_1 = pd.DataFrame(source_df.groupby(inputs_required[0])[
                                inputs_required[1]].count())
                            or_df_1.reset_index(level=0, inplace=True)
                            or_df_2 = pd.DataFrame(source_df.groupby(inputs_required[1])[
                                inputs_required[0]].count())
                            or_df_2.reset_index(level=0, inplace=True)
                            or_df_1 = or_df_1[or_df_1[inputs_required[1]] > 1]
                            or_df_2 = or_df_2[or_df_2[inputs_required[0]] > 1]
                            if (source_df.groupby(inputs_required[0])[
                                inputs_required[1]].count().max()>1) \
                            and (source_df.groupby(inputs_required[1])[
                                inputs_required[0]].count().max()==1):
                                out_df = source_df.merge(or_df_1,on = [
                                    inputs_required[0]],how='inner')
                                dups = []
                                for i,row in out_df.iterrows():
                                    dups.append(row[inputs_required[0]]+ '|' +row[
                                        inputs_required[1]+'_x'])
                            elif (source_df.groupby(inputs_required[1])[inputs_required[
                                0]].count().max()>1) and (source_df.groupby(inputs_required[0])[
                                    inputs_required[1]].count().max() == 1):
                                out_df = source_df.merge(or_df_2,on=[inputs_required[1]],
                                how='inner')
                                dups = []
                                for i,row in out_df.iterrows():
                                    dups.append(row[inputs_required[0]+'_x'] + '|' + row[
                                        inputs_required[1]])
                            elif (source_df.groupby(inputs_required[1])[inputs_required[
                                0]].count().max()>1) and (source_df.groupby(inputs_required[
                                    0])[inputs_required[1]].count().max() > 1):
                                out_df_1 = source_df.merge(or_df_1,on = [inputs_required[0]],
                                how='inner')
                                dups_1 = []
                                for i,row in out_df_1.iterrows():
                                    dups_1.append(row[inputs_required[0]] + '|' + row[
                                        inputs_required[1]+'_x'])
                                out_df_2 = source_df.merge(or_df_2,on = [inputs_required[1]],
                                how='inner')
                                dups_2 = []
                                for i,row in out_df_2.iterrows():
                                    dups_2.append(row[inputs_required[0]+'_x'] + '|' + row[
                                        inputs_required[1]])
                                dups = set(dups_1 + dups_2)
                            output_df.at[index,'output_reference'] = f'{dups}'
                            log2.info('QC for %s check %s|%s|%s', checks_name,
                            inputs_required[0], inputs_required[1], output_df.at[index, "result"])
                        output_df.at[index, 'end_time'] = datetime.now().strftime(
                            "%d/%m/%Y %H:%M:%S")
                    #multi to one mapping
                    elif checks_name == 'multi_to_one_mapping':
                        sr_df = main_json_file['task']['source']['source_file_path']+ \
                        src_file_name+'.csv'
                        source_df1 = pd.read_csv(sr_df, na_filter=False)
                        source_df1['compound_check_column'] = source_df1[inputs_required[0]]+'|'+ \
                        source_df1[inputs_required[1]]
                        if source_df1.shape[0] == source_df1['compound_check_column'].nunique():
                            if source_df1.shape[0] == source_df1[inputs_required[0]].nunique():
                                output_df.at[index,'result'] = 'PASS'
                                log2.info('QC for %s check %s|%s|%s', checks_name,
                            inputs_required[0], inputs_required[1], output_df.at[index, "result"])
                            else:
                                output_df.at[index,'result'] = 'FAIL'
                                list1 = source_df1[inputs_required[0]].tolist()
                                duplicates = [i for i in list1 if list1.count(i)>1]
                                dups = []
                                for row in source_df1['compound_check_column'].tolist():
                                    if row.rsplit('|',1)[0] in duplicates:
                                        dups.append(row)
                                output_df.at[index,'output_reference'] = f'{dups}'
                                log2.info('QC for %s check %s|%s|%s', checks_name,
                            inputs_required[0], inputs_required[1], output_df.at[index, "result"])
                        else:
                            output_df.at[index,'result'] = 'FAIL'
                            list1 = source_df1['compound_check_column'].tolist()
                            duplicates = [i for i in list1 if list1.count(i)>1]
                            output_df.at[index,'output_reference'] = \
                            f'The combination of {inputs_required[0]} and {inputs_required[1]} \
                             is not unique - {duplicates}'
                            log2.info('QC for %s check %s|%s|%s', checks_name,
                            inputs_required[0], inputs_required[1], output_df.at[index, "result"])
                    output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return output_df
    except ValueError:
        log2.error("func name(check_name) mentioned in the json is incorrect please check it.")
        write_to_txt1(task_id,'FAILED',file_path)
        audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
        sys.exit()
    except Exception as error:
        write_to_txt1(task_id,'FAILED',file_path)
        audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
        log2.exception("error in run_checks_in_parallel function %s.", str(error))
        raise error

def qc_check(prj_nm,control_table_df, checks_mapping_df, src_file_name, check_type, ing_type,
loc,encoding,sheetnum, conn_str,main_json_file,task_id,run_id, paths_data,file_path,audit_json_path,
dq_output_loc=None):
    """Extaracting qc_check related details"""
    try:
        control_table_df = control_table_df[control_table_df[
            'type'] == check_type].reset_index(drop=True)
        cols = control_table_df.columns.tolist()
        resultset = pd.DataFrame(
            columns=cols + [
                'unexpected_index_list', 'threshold_voilated_flag', 'run_flag', 'result',
                'output_reference', 'start_time', 'end_time', 'good_records_file',
                 'bad_records_file', 'good_records_count', 'bad_records_count'])
        row_val = control_table_df.index.values.tolist()
        pool = ThreadPool(multiprocessing.cpu_count())
        #Creating conditions for different file formats
        try:
            if ing_type in {'csv_read', 'csv_write'}:
                pd_df = pd.read_csv(loc, encoding=encoding)
                ge_df = ge.from_pandas(pd_df)
                shape_of_records1 = ge_df.shape
                log2.info('Reading csv file started at %s', loc)
                log2.info("printing file path:%s",file_path)
                log2.info(
                'Total number of records present in above path are %s', shape_of_records1)
            elif ing_type in {'parquet_read', 'parquet_write'}:
                ge_df = ge.read_parquet(loc)
                shape_of_records2 = ge_df.shape
                log2.info('Reading parquet file started at %s', loc)
                log2.info(
                    'Total number of records present in above path are %s', shape_of_records2)
            elif ing_type in {'json_read', 'json_write'}:
                ge_df = ge.read_json(loc, encoding=encoding)
                shape_of_records3 = ge_df.shape
                log2.info('Reading json file started at %s', loc)
                log2.info(
                    'Total number of records present in above path are %s', shape_of_records3)
            elif ing_type in {'excel_read', 'excel_write'}:
                ge_df = ge.read_excel(loc, sheet_name=sheetnum)
                shape_of_records4 = ge_df.shape
                log2.info('Reading excel file started at %s', loc)
                log2.info(
                    'Total number of records present in above path are %s', shape_of_records4)
            elif ing_type in {'xml_read', 'xml_write'}:
                pd_df = pd.read_xml(loc)
                ge_df = ge.from_pandas(pd_df)
                shape_of_records5 = ge_df.shape
                log2.info('Reading xml file started at %s', loc)
                log2.info(
                    'Total number of records present in above path are %s', shape_of_records5)
        except FileNotFoundError:
            log2.error("the input source file  does not exists or not found")
            write_to_txt1(task_id,'FAILED',file_path)
            audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
            sys.exit()
        try:
            decrypt_path=paths_data["folder_path"]+paths_data["ingestion_path"]
            sys.path.insert(0, decrypt_path)
            from utility import decrypt
            if ing_type in {'postgres_read', 'postgres_write'}:
                log2.info("entered into postgres read")
                password = decrypt(conn_str['password'])
                conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
                    f':{password.replace("@", "%40")}@{conn_str["host"]}'
                    f':{int(conn_str["port"])}/{conn_str["database"]}', encoding='utf-8')
                pd_df = pd.read_sql(f'select * from {loc}', conn)
                ge_df = ge.from_pandas(pd_df)
                connection = conn.raw_connection()
                cursor = connection.cursor()
                sql = f'SELECT count(0) from  {loc};'
                cursor.execute(sql)
                myresult = cursor.fetchall()
                log2.info('Reading postgres db started at %s table', loc)
                log2.info(
                    'Total number of records present in above table are %s', myresult[-1][-1])
            elif ing_type in {'mysql_read', 'mysql_write'}:
                password = decrypt(conn_str['password'])
                conn = sqlalchemy.create_engine(f'mysql+pymysql://{conn_str["user"]}'
                f':{password.replace("@", "%40")}@{conn_str["host"]}'
                f':{int(conn_str["port"])}/{conn_str["database"]}', encoding='utf-8')
                if main_json_file['task']['source']['query'] == " ":
                    loc = main_json_file['task']['source']['table_name']
                    pd_df = pd.read_sql(f'select * from ({loc})', conn)
                    log2.info('Reading mysql db started at %s table', loc)
                    ge_df = ge.from_pandas(pd_df)
                    connection = conn.raw_connection()
                    cursor = connection.cursor()
                    sql = f'SELECT count(0) from  ({loc})'
                else:
                    loc = main_json_file['task']['source']['query']
                    pd_df = pd.read_sql(f'{loc}', conn)
                    ge_df = ge.from_pandas(pd_df)
                    connection = conn.raw_connection()
                    cursor = connection.cursor()
                    sql = f'SELECT count(0) from  ({loc}) as d;'
                cursor.execute(sql)
                myresult = cursor.fetchall()
                log2.info(
                    'Total number of records present in above table are %s', myresult[-1][-1])
            elif ing_type in {'snowflake_read', 'snowflake_write'}:
                password = decrypt(conn_str['password'])
                conn = sqlalchemy.create_engine(f'snowflake://{conn_str["user"]}'
                f':{password.replace("@", "%40")}@{conn_str["account"]}/'
                f':{conn_str["database"]}/{conn_str["schema"]}'
                f'?warehouse={conn_str["warehouse"]}&role={conn_str["role"]}')
                if main_json_file['task']['source']['query'] == " ":
                    loc = main_json_file['task']['source']['table_name']
                    pd_df = pd.read_sql(f'select * from ({loc})', conn)
                    log2.info('Reading snowflake db started at %s table', loc)
                    ge_df = ge.from_pandas(pd_df)
                    connection = conn.raw_connection()
                    cursor = connection.cursor()
                    sql = f'SELECT count(0) from  ({loc})'
                else:
                    loc = main_json_file['task']['source']['query']
                    pd_df = pd.read_sql(f'{loc}', conn)
                    ge_df = ge.from_pandas(pd_df)
                    connection = conn.raw_connection()
                    cursor = connection.cursor()
                    sql = f'SELECT count(0) from  ({loc}) as d;'
                cursor.execute(sql)
                myresult = cursor.fetchall()
                log2.info(
                    'Total number of records present in above table are %s', myresult[-1][-1])
        except sqlalchemy.exc.ProgrammingError:
            log2.error("the table or connection specified in the command "
            " is incorrect")
            write_to_txt1(task_id,'FAILED',file_path)
            audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
            sys.exit()
        except sqlalchemy.exc.OperationalError:
            log2.error("The details provided inside the connection file path "
            " is incorrect")
            write_to_txt1(task_id,'FAILED',file_path)
            audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
            sys.exit()
        except Exception as error:
            write_to_txt1(task_id,'FAILED',file_path)
            audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
            log2.exception("error in qc_check function %s.", str(error))
            raise error
        #else:
            #raise Exception("Not a valid ingestion type")
        datasets = pool.map(
            lambda x:run_checks_in_parallel(
            x, cols, control_table_df, checks_mapping_df, ge_df, main_json_file, task_id,
            run_id,file_path,audit_json_path), row_val)
        pool.close()
        for datas in datasets:
            resultset = pd.concat([resultset, datas])
        resultset['unexpected_index_list'] = resultset['unexpected_index_list'].replace(np.nan,'')
        bad_records_indexes = list({
            item for sublist in resultset['unexpected_index_list'].tolist() for item in sublist})
        bad_file_loc = paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["rejected_path"]
        if 'FAIL' in resultset.result.values:
            indexes = list(set(bad_records_indexes))
            bad_records_df = ge_df[ge_df.index.isin(indexes)]
            no_of_bad_records = bad_records_df.shape[0]
            log2.info('Total number of bad records are %s', no_of_bad_records)
            bad_records_df.to_csv(
                bad_file_loc + src_file_name +'_' + 'rejected_records_' +
                    datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
            if 'Y' in resultset['threshold_voilated_flag']:
                good_records_df = pd.DataFrame(columns=ge_df.columns.tolist())
                #accepted_records section
                good_records_df.to_csv(
                    dq_output_loc + src_file_name + '.csv', index=False)
            else:
                good_records_df = ge_df[~ge_df.index.isin(indexes)]
                no_of_good_records = good_records_df.shape[0]
                log2.info('Total number of good records are %s', no_of_good_records)
                #accepted_records section
                good_records_df.to_csv(
                    dq_output_loc + src_file_name + '.csv', index=False)
        else:
            resultset['good_records_file'] = ""
            resultset['bad_records_file'] = ""
        resultset['good_records_file'] = dq_output_loc +\
            src_file_name + '_' + datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv'
        resultset['bad_records_file'] = dq_output_loc +\
            src_file_name + '_' + datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv'
        resultset = resultset.drop(
        columns = ['unexpected_index_list', 'threshold_voilated_flag', 'run_flag'])
        return resultset
    except Exception as error:
        write_to_txt1(task_id,'FAILED',file_path)
        audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
        log2.exception("error in qc_check function %s.", str(error))
        raise error

def auto_correction(main_json_file):
    '''function to perform auto correction'''
    log2.info("Auto correction process started")
    output_loc = main_json_file['task']['source']['source_file_path']
    src_file_name = main_json_file['task']['source']['source_file_name']
    df1 = pd.read_csv(output_loc+main_json_file['task']['source'][
        'source_file_name']+'.csv')
    df2 = pd.read_csv(
        main_json_file['task']['data_quality_features']['dq_lookup_file_path'])
    #Creating Temprary new column(id) in source dataframe
    df1.insert(0, 'temp_id', range(0, 0 + len(df1)))
    #Assigning key_col variable to perform the key_column operation
    key_col = main_json_file['task']['data_quality_features']['dq_lkup_key_column']
    #merging both dataframe to replace the null values
    for s_col, t_col in key_col.items():
        res=pd.merge(df1, df2, left_on=s_col, right_on=t_col)
        res1 = res.drop(columns=[t_col])
    var = main_json_file['task']['data_quality_features']['dq_auto_correct_columns']
    for i,j in var.items():
        res1[i] = res1[i].fillna(res[j])
        res1.sort_values(by=['temp_id'],ascending=True,inplace=True)
        res1.pop(j)
        res2 = res1.drop(columns=['temp_id'])
    os.makedirs(output_loc, exist_ok=True)
    res2.to_csv(output_loc+ src_file_name + '.csv', index=False)
    log2.info("Generates auto_corrected_records file at %s", output_loc)
    log2.info("Auto correction process ended")

def data_masking(main_json_file):
    '''Function to perform data_masking'''
    log2.info("Data Masking Operation Started")
    src_file_name=main_json_file['task']['source']['source_file_name']
    i_r=main_json_file['task']['data_quality_features']['data_masking_columns']
    val = i_r.values()
    msk_date = list(val)[0].split(',')
    msk_alpha = list(val)[1].split(',')
    msk_numeric = list(val)[2].split(',')
    #extracting key values to fix the date alpha and numeric values
    result_keys = list(i_r.keys())
    output_loc = main_json_file['task']['source']['source_file_path']
    filename = output_loc+src_file_name+'.csv'
    srcdf = pd.read_csv(filename)
    #masking the date columns
    if result_keys[0] == 'msk_date':
        default_date_pattern = '0' if msk_date[0]=='' else msk_date[0]
        for i in msk_date[1:]:
            if i == "":
                pass
            else:
                srcdf[i]=srcdf[i].replace(r'\w', default_date_pattern, regex=True)
    #making the alpha-numeric columns
    if result_keys[1] == 'msk_alpha':
        default_alpha_pattern = '*' if msk_alpha[0]=='' else msk_alpha[0]
        for j in msk_alpha[1:]:
            if j == "":
                pass
            else:
                srcdf[j]=srcdf[j].replace(r'\w', default_alpha_pattern, regex=True)
    #masking the numeric columns
    if result_keys[2] == 'msk_numeric':
        if msk_numeric=='':
            pass
        else:
            srcdf[msk_numeric] = np.nan
    else:
        print('Error in data_masking_column formats.')
    srcdf.to_csv(output_loc+ src_file_name +'.csv',encoding='utf-8', index=False)
    log2.info("generates data_masking_file at: %s", output_loc)
    log2.info("Data masking Operation Ended")

def data_encryption(main_json_file):
    '''function to perform data_encryption'''
    log2.info("Data Encryption Started")
    src_file_name=main_json_file['task']['source']['source_file_name']
    output_loc = main_json_file['task']['source']['source_file_path']
    cols_required = main_json_file['task']['data_quality_features'][
                    'data_encryption_columns']
    cols=cols_required
    source_file = output_loc+ src_file_name +'.csv'
    encrypt_df = pd.read_csv(source_file)
    for val in cols.split(','):
        for vals in encrypt_df[val]:
            res1 = vals
            res = encrypt(vals)
            encrypt_df[val] = encrypt_df[val].replace(res1, res)
    encrypt_df.to_csv(output_loc+ src_file_name +'.csv',encoding='utf-8', index=False)
    log2.info("Generates data_encrypted_records file at %s", output_loc)
    log2.info("Data Encryption Completed")


def qc_pre_check(prj_nm,main_json_file,cm_json_file,paths_data,config_file_path,task_id,run_id,
                 file_path,audit_json_path):
    """Function to perform pre_check operation"""
    try:
        #To read task json file to extract important deatils
        control_table = pd.DataFrame(main_json_file['task']['data_quality'])
        get_config_section_path=paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, get_config_section_path)
        from utility import get_config_section
        checks_mapping = pd.DataFrame(cm_json_file['checks_mapping'])
        output_loc = paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["source_files_path"]
        try:
            if main_json_file['task']['source']['source_type'] in {'postgres_read', 'mysql_read',
            'snowflake_read'}:
                src_conn_str =  get_config_section(
                config_file_path+main_json_file["task"]['source']["connection_name"]+'.json',
                main_json_file['task']['source']['connection_name']) if main_json_file[
                'task']['source']['connection_name'] != '' else ''
                src_file_name = main_json_file['task']['source']['table_name']
            else:
                src_conn_str='None'
                src_file_name = main_json_file['task']['source']['source_file_name']
        except KeyError:
            log2.error('Connection name might incorrect check once')
            sys.exit()
        if main_json_file['task']['source']['source_type'] in {'csv_read', 'json_read'}:
            default_encoding = 'utf-8' if main_json_file['task']['source']['encoding']==' ' else \
                main_json_file['task']['source']['encoding']
        else:
            default_encoding='None'
        if main_json_file['task']['source']['source_type'] == 'excel_read':
            default_sheetnum = '0' if main_json_file['task']['source']['sheet_name']=='' else \
                main_json_file['task']['source']['sheet_name']
        else:
            default_sheetnum='None'
        # defaault file path based on source_type
        if main_json_file['task']['source']['source_type'] == 'csv_read':
            def_loc = main_json_file['task']['source']['source_file_path']+main_json_file[
                                            'task']['source']['source_file_name']+'.csv'
        elif main_json_file['task']['source']['source_type'] == 'parquet_read':
            def_loc = main_json_file['task']['source']['source_file_path']+main_json_file[
                                            'task']['source']['source_file_name']+'.parquet'
        elif main_json_file['task']['source']['source_type'] == 'excel_read':
            def_loc = main_json_file['task']['source']['source_file_path']+main_json_file[
                                            'task']['source']['source_file_name']+'.xlsx'
        elif main_json_file['task']['source']['source_type'] == 'json_read':
            def_loc = main_json_file['task']['source']['source_file_path']+main_json_file[
                                            'task']['source']['source_file_name']+'.json'
        #if table name is not present in the json then it has to take the values from query
        if main_json_file['task']['source']['query'] == " ":
            src_tbl_name = main_json_file['task']['source']['table']
        else:
            src_tbl_name = main_json_file['task']['source']['query']
        #if all the data_quality features set to N then precheck operation started
        if main_json_file['task']['data_quality_features']['dq_auto_correction_required']=='N' and \
        main_json_file['task']['data_quality_features']['data_masking_required'] == 'N' and \
        main_json_file['task']['data_quality_features']['data_encryption_required'] == 'N' and \
        main_json_file['task']['data_quality_features']['data_decryption_required'] == 'N':
            log2.info('Pre-Check Operation Started')
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
            'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                prj_nm,control_table, checks_mapping, main_json_file['task']['source'][
                'source_file_name'],'pre_check', main_json_file['task']['source']['source_type'],
                def_loc, default_encoding, default_sheetnum,src_conn_str,main_json_file,task_id,
                run_id,paths_data,file_path,audit_json_path,output_loc)
            elif main_json_file['task']['source']['source_type'] in {'postgres_read', 'mysql_read'}:
                pre_check_result = qc_check(
                    prj_nm, control_table, checks_mapping, main_json_file['task']['source'][
                    'table_name'],'pre_check', main_json_file['task'][
                    'source']['source_type'], src_tbl_name,default_encoding, default_sheetnum, 
                    src_conn_str,main_json_file,task_id,run_id,paths_data,file_path,
                    audit_json_path,output_loc)
            elif main_json_file['task']['source']['source_type'] in {'snowflake_read'}:
                pre_check_result = qc_check(
                    prj_nm, control_table, checks_mapping, main_json_file['task']['source'][
                    'table_name'],'pre_check', main_json_file['task'][
                    'source']['source_type'], src_conn_str["database"]+'.'+main_json_file[
                    'task']['source']['schema']+'.'+src_tbl_name,default_encoding, default_sheetnum, 
                    src_conn_str,main_json_file,task_id,run_id,paths_data,file_path,audit_json_path,
                    output_loc)
        #both auto_correction, data_masking and data_encryption enabling
        if main_json_file['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
        and main_json_file['task']['data_quality_features']['data_masking_required'] == 'Y' \
        and main_json_file['task']['data_quality_features']['data_encryption_required'] == 'Y':
            auto_correction(main_json_file)
            data_masking(main_json_file)
            data_encryption(main_json_file)
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
            'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                prj_nm, control_table, checks_mapping, src_file_name,'pre_check',
                main_json_file['task']['source']['source_type'], def_loc,
                default_encoding, default_sheetnum,src_conn_str, main_json_file,task_id,
                run_id, paths_data,file_path,audit_json_path,output_loc)
        # both auto_correction and data_masking enabling
        elif main_json_file['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
        and main_json_file['task']['data_quality_features']['data_masking_required'] == 'Y':
            auto_correction(main_json_file)
            data_masking(main_json_file)
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
                'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                prj_nm, control_table, checks_mapping, src_file_name,'pre_check',
                main_json_file['task']['source']['source_type'], def_loc,
                default_encoding, default_sheetnum,src_conn_str,main_json_file,task_id,
                run_id, paths_data,file_path,audit_json_path,output_loc)
        #both auto_correction and data_encryption enabling
        elif main_json_file['task']['data_quality_features']['dq_auto_correction_required'] == 'Y' \
        and main_json_file['task']['data_quality_features']['data_encryption_required'] == 'Y':
            auto_correction(main_json_file)
            data_encryption(main_json_file)
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
                'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                prj_nm, control_table, checks_mapping, src_file_name,'pre_check',
                main_json_file['task']['source']['source_type'], def_loc,
                default_encoding, default_sheetnum,src_conn_str, main_json_file,task_id,
                run_id, paths_data,file_path,audit_json_path,output_loc)
        elif main_json_file['task']['data_quality_features']['dq_auto_correction_required'] == 'Y':
            # calling auto_correction function
            auto_correction(main_json_file)
            log2.info("Pre_check operation started")
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
                'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                prj_nm, control_table, checks_mapping, src_file_name,'pre_check',
                main_json_file['task']['source']['source_type'], def_loc,
                default_encoding, default_sheetnum,src_conn_str, main_json_file,task_id,
                run_id, paths_data,file_path,audit_json_path,output_loc)
            #elif main_json_file['task']['source']['source_type'] in {
            # 'postgres_read', 'mysql_read'}:
            #     pre_check_result = qc_check(
            #         control_table, checks_mapping, main_json_file['task']['source'][
            #         'table_name'],'pre_check', main_json_file['task'][
            #         'source']['source_type'], main_json_file['task']['source'][
            #         'table_name'], default_encoding, default_sheetnum, src_conn_str,
            #         main_json_file,task_id,status,run_id,path,audit_json_path,output_loc)
            log2.info("Pre-Check Operation completed")
        #Data Masking
        elif main_json_file['task']['data_quality_features']['data_masking_required'] == 'Y':
            data_masking(main_json_file)
            log2.info("Pre-Check Operation Started")
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
                'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                prj_nm, control_table, checks_mapping, main_json_file['task']['source'][
                'source_file_name'],'pre_check', main_json_file['task'][
                'source']['source_type'], def_loc, default_encoding, default_sheetnum,src_conn_str,
                main_json_file,task_id,run_id, paths_data,file_path,audit_json_path,output_loc)
            #elif main_json_file['task']['source']['source_type'] in {'postgres_read',
            # 'mysql_read'}:
            #     pre_check_result = qc_check(
            #         control_table, checks_mapping, main_json_file['task']['source'][
            #         'table_name'],'pre_check', main_json_file['task'][
            #         'source']['source_type'], main_json_file['task']['source'][
            #         'table_name'], default_encoding, default_sheetnum,
            #         src_conn_str, main_json_file,task_id,run_id, path, output_loc,file_path)
            log2.info("Pre-Check Operation completed")
        #Data Encryption
        elif main_json_file['task']['data_quality_features']['data_encryption_required']=='Y':
            data_encryption(main_json_file)
            log2.info("Pre-Check Operation Started")
            if main_json_file['task']['source']['source_type'] in {'csv_read', 'parquet_read', \
                'excel_read', 'xml_read', 'json_read'}:
                pre_check_result = qc_check(
                    prj_nm, control_table, checks_mapping, main_json_file['task']['source'][
                    'source_file_name'],'pre_check', main_json_file['task'][
                    'source']['source_type'], def_loc, default_encoding,
                    default_sheetnum, src_conn_str, main_json_file,task_id,run_id,
                    paths_data,file_path,audit_json_path, output_loc)
            # elif main_json_file['task']['source']['source_type'] in {'postgres_read',
            # 'mysql_read'}:
            #     pre_check_result = qc_check(
            #     control_table, checks_mapping, main_json_file['task']['source']['table_name'],
            #     'pre_check', main_json_file['task']['source']['source_type'], main_json_file[
            #     'task']['source']['table_name'], default_encoding, default_sheetnum,src_conn_str,
            #      main_json_file,task_id,run_id,path,file_path,audit_json_path, output_loc)
            log2.info("Pre_check operation completed")
        return pre_check_result
    except Exception as error:
        write_to_txt1(task_id,'FAILED',file_path)
        audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
        log2.exception("error in qc_pre_check function %s.", str(error))
        raise error

def qc_post_check(prj_nm,main_json_file,cm_json_file,paths_data,config_file_path, task_id,run_id,
                  file_path,audit_json_path):
    """Function to perform post_check operation"""
    try:
        control_table = pd.DataFrame(main_json_file['task']['data_quality'])
        get_config_section_path=paths_data["folder_path"]+paths_data["ingestion_path"]
        sys.path.insert(0, get_config_section_path)
        from utility import get_config_section
        checks_mapping = pd.DataFrame(cm_json_file['checks_mapping'])
        try:
            if main_json_file['task']['target']['target_type'] in {'postgres_write', 'mysql_write',
            'snowflake_write'}:
                tgt_conn_str =  get_config_section(
                    config_file_path+main_json_file["task"]['target']["connection_name"]+'.json',
                    main_json_file['task']['target']['connection_name']) if main_json_file[
                        'task']['target']['connection_name'] != '' else ''
            else:
                tgt_conn_str = 'None'
        except KeyError:
            log2.error('Connection name might incorrect check once')
        if main_json_file['task']['target']['target_type'] in {'csv_write', 'json_write'}:
            default_encoding = 'utf-8' if main_json_file['task']['target']['encoding']=='' else \
                main_json_file['task']['target']['encoding']
        else:
            default_encoding='None'
        if main_json_file['task']['target']['target_type'] == 'excel_write':
            default_sheetnum = '0' if main_json_file['task']['target']['sheet_name']=='' else \
                main_json_file['task']['target']['sheet_name']
        else:
            default_sheetnum='None'
        log2.info("Post_check operation started")
        output_loc = paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["rejected_path"]
        # src_file_name = main_json_file['task']['target']['table_name']
        # Reprocessing of bad records file
        if main_json_file['task']['data_quality_features']['data_decryption_required']=='N':
            if main_json_file['task']['target']['target_type'] in {'csv_write',
            'parquet_write', 'excel_write', 'xml_write', 'json_write'}:
                post_check_result = qc_check(
                prj_nm, control_table, checks_mapping, main_json_file['task']['target'][
                'target_file_name'],'post_check', main_json_file['task'][
                'target']['target_type'], main_json_file['task']['target'][
                'target_file_path']+main_json_file['task']['target'][
                'target_file_name'], default_encoding, default_sheetnum,tgt_conn_str, 
                main_json_file,task_id,run_id, paths_data,file_path,audit_json_path, output_loc)
            elif main_json_file['task']['target']['target_type'] in {'postgres_write',
            'mysql_write'}:
                post_check_result = qc_check(
                prj_nm, control_table, checks_mapping, main_json_file['task']['target'][
                'table_name'],'post_check', main_json_file['task'][
                'target']['target_type'], main_json_file['task']['target'][
                'table_name'], default_encoding, default_sheetnum,tgt_conn_str,
                main_json_file,task_id,run_id, paths_data,file_path,audit_json_path,output_loc)
            elif main_json_file['task']['target']['target_type'] in {'snowflake_write'}:
                post_check_result = qc_check(
                prj_nm, control_table, checks_mapping, main_json_file['task']['target'][
                'table_name'],'post_check', main_json_file['task'][
                'target']['target_type'], tgt_conn_str["database"]+'.'+main_json_file[
                'task']['target']['schema']+'.'+main_json_file['task']['target'][
                'table_name'], default_encoding, default_sheetnum, tgt_conn_str,
                main_json_file,task_id,run_id,paths_data,file_path,audit_json_path,output_loc)
        elif main_json_file['task']['data_quality_features']['data_decryption_required']=='Y':
            cols_required = main_json_file['task']['data_quality_features'][
            'data_decryption_columns']
            cols=cols_required
            src_file_name = main_json_file['task']['source']['source_file_nmae']
            output_file_path =main_json_file['task']['data_quality_features']['dq_output_file_path']
            list_of_files = glob.glob(f'{output_file_path}*.csv')
            list_of_encrypted_files = list(filter(lambda f: f.startswith(
                f'{output_file_path}{src_file_name}'), list_of_files))
            source_file = max(list_of_encrypted_files, key=os.path.getctime, default=None)
            decrypt_df = pd.read_csv(source_file)
            decrypt_path=output_file_path+paths_data["engine_path"]
            sys.path.insert(0, decrypt_path)
            from utility import decrypt
            for val in cols.split(','):
                for vals in decrypt_df[val]:
                    res1 = vals
                    res = decrypt(vals)
                    decrypt_df[val] = decrypt_df[val].replace(res1, res)
            decrypt_df.to_csv(output_loc+ src_file_name +'_'+ 'data_decrypted_records_' +
                datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.csv',encoding='utf-8', index=False)
            list_of_files = glob.glob(f'{output_loc}*.csv')
            data_decrypted_file = max(list_of_files, key=os.path.getctime, default='None')
            if main_json_file['task']['target']['target_type'] in {'csv_write', 'parquet_write', \
            'excel_write', 'xml_write', 'json_write'}:
                post_check_result = qc_check(
                    control_table, checks_mapping, main_json_file['task']['target'][
                    'target_file_name'],'post_check', main_json_file['task'][
                    'target']['target_type'], data_decrypted_file, default_encoding,
                    default_sheetnum, tgt_conn_str, main_json_file,task_id,run_id,
                    output_file_path,file_path,audit_json_path,output_loc)
            elif main_json_file['task']['target']['target_type'] in {'postgres_write',
            'mysql_write', 'snowflake_write'}:
                post_check_result = qc_check(
                control_table, checks_mapping, main_json_file['task']['target'][
                'table_name'],'post_check', main_json_file['task'][
                'target']['target_type'], main_json_file['task']['target'][
                'table_name'], default_encoding, default_sheetnum,tgt_conn_str,
                main_json_file,task_id,run_id,output_file_path,file_path,audit_json_path,output_loc)
        log2.info("Post_check operation completed")
        return post_check_result
    except Exception as error:
        write_to_txt1(task_id,'FAILED',file_path)
        audit(audit_json_path,main_json_file, task_id,run_id,'STATUS','FAILED')
        log2.error("error in qc_post_check function %s.", str(error))
        raise error

def qc_report(pre_check_result,post_check_result,new_path,file_path,audit_json_path,json_data,
              task_id,run_id):
    """Function to generate qc_report"""
    try:
        #Concatinating the pre_check and post_check results into final_check_result
        output_loc = new_path
        final_check_result = pd.concat([pre_check_result, post_check_result], axis=0)
        final_check_result = final_check_result.reset_index(drop=True)
        final_check_result.to_csv(output_loc + 'qc_report_' + datetime.now().strftime(
        "%d_%m_%Y_%H_%M_%S") + '.csv',index=False)
        log2.info("qc_report generated")
        return final_check_result
    except Exception as error:
        write_to_txt1(task_id,'FAILED',file_path)
        audit(audit_json_path,json_data, task_id,run_id,'STATUS','FAILED')
        log2.exception("error in qc_report function %s.", str(error))
        raise error

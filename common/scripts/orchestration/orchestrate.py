""" importing modules """
import json
import logging
import time
import sys
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os import path
import multiprocessing as mp
import pandas as pd
import download

log1 = logging.getLogger('log1')

def execute_job(prj_nm,paths_data,task_id,pip_id,run_id):
    """function for master_executor calling"""
    try:
        logging_path= paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["task_log_path"]
        logging.info(logging_path)
        logging.info("inside master executor")
        download.execute_engine(prj_nm,task_id,paths_data,run_id,pip_id)
    except Exception as error:
        logging.exception("error in master_executor file %s.", str(error))
        raise error

def orchestration_execution(prj_nm,paths_data,pip_nm,run_id):
    """function for executing orchestration process"""
    # Readding Pipeline json file
    try:
        new_path= paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
        paths_data["pipeline_json_path"]+ pip_nm+ '.json'
        with open(r""+new_path,"r", encoding='utf-8') as jsonfile:
            json_data = json.load(jsonfile)
            task_details=json_data['tasks_details'].items()
            pd.DataFrame(json_data['tasks_details'].items())
    except Exception as error:
        log1.exception("error in reading pipeline json %s.", str(error))
        raise error

    log1.info('Pipeline execution initiated')
    #Script starts here
    independent_task=[]
    dependent_task=[]
    new_dep_task_list=[]
    for key, value in task_details:
        if value == 0:
            independent_task.append(key)
        elif value != 0:
            dependent_task.append(key)
            new_dep_task_list.append(key)

    file_path=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
    paths_data["status_txt_file_path"]+run_id+".txt"

    #Running the independent jobs in a loop###
    ind_processes = []
    is_break='N'
    for i in independent_task:
        log1.info('triggering the task run %s', i)
        # lock =mp.Lock()
        ind_task = mp.Process(target = execute_job, args = [prj_nm,paths_data,i,pip_nm,run_id],\
                           name = 'Process_' + str(i))
        ind_processes.append(ind_task)
        log1.info('Starting process %s' ,str(ind_task.name))
        ind_task.start()
        # time.sleep(5)
        # log1.info("ind_processes list: %s", ind_processes)
    for process in ind_processes:
        process.join()
        # log1.info("process joined:%s",process)


    #Running the dependent jobs in a loop
    length = int(len(dependent_task))
    dep_processes = []
    while length>0:
        for row in dependent_task:
            df_2 =pd.read_csv(file_path, sep='\t')
            success_ls=df_2[df_2['Job_Status'] == 'SUCCESS']['task_name'].to_list()
            failed_ls=df_2[df_2['Job_Status'] == 'FAILED']['task_name'].to_list()
            task_depend=df_2[df_2['task_name'] == row]['task_depended_on'].to_list()
            if (len(failed_ls) != 0) & (set(failed_ls).issubset(set(task_depend)) |\
            set(task_depend).issubset(set(failed_ls)) is True ):
                log1.warning("Task failed so stopping execution of %s", row )
                is_break='Y'
                break
            if set(task_depend).issubset(set(success_ls)) is False:
                time.sleep(3)
                continue
            else:
                log1.info('triggering the task run %s', row)
                # execute_job(paths_data,row,pip_nm,run_id)
                dep_task = mp.Process(target = execute_job, args = [prj_nm,paths_data,row,
                pip_nm,run_id], name = 'Process_' + str(row))
                dep_processes.append(dep_task)
                log1.info('Starting process %s' ,str(dep_task.name))
                dep_task.start()
                dependent_task.remove(row)
                length=length-1
                for process in dep_processes:
                    process.join()
        if is_break=='Y':
            break

    #reading the orchestration text file for getting the statuses
    df_2 =pd.read_csv(file_path, sep='\t')
    # loop for independent tasks
    for i, row in df_2.iterrows():
        if str(row['task_depended_on'])=='0':
            log1.info(str(row['task_name']+" task is "+row['Job_Status']))

    pipeline_status_list = df_2['Job_Status'].to_list()
    #loop for dependent tasks
    # dependent_task =set(df_2[df_2['task_depended_on'] != 0]['task_name'].to_list())
    for i in new_dep_task_list:
        status = df_2[df_2['task_name'] == i]['Job_Status'].to_list()
        if 'FAILED' in status:
            log1.info("%s task is FAILED", i)
        elif 'Start' in status:
            log1.info("%s task is NOT STARTED", i)
        else :
            log1.info("%s task is SUCCESS", i)
    return pipeline_status_list

###################################################################################################
#   This Function flatten the table to 1:1
#   Means if the `task_depended_on` has multiple jobs, then `task_name` will have multiple rows
#   Run the function and check the source and output result.
###################################################################################################

def df_flatten(df_process):
    'This function flatten the composite dataframe to contain parent and child job'
    df_flat = pd.DataFrame(columns=["task_name","task_depended_on"])
    for _, row in df_process.iterrows():
        if row[1]==0:
            x_1=list(str(row[1]))
        else:
            x_1=row[1]
        # print(x)
        if len(x_1) == 1 or x_1==0:
            df_flat.loc[df_flat.shape[0]] = [row[0], x_1[0]]
        else:
            for i in x_1:
                df_flat.loc[df_flat.shape[0]] = [row[0], i]
    # print(df_flat)
    return df_flat

##################################################################################################
#   This Function will traverse the all task_name -> task_depended_on -> task_name ->
# on and on till end
#   Means if the `task_depended_on` has multiple jobs, then `task_name` will have
# multiple rows
#   while traversing, If a node is already discovered then exception `Cyclic decteded`,
#  else success.
#   This is a recusion function to find last node or already visited node.
#################################################################################################

def node_visit(df_flat, u_1, discovered, finished):
    """checking cyclic dependency"""
    discovered.add(u_1)

    next_tasks = df_flat[df_flat['task_depended_on'] == u_1]['task_name'].to_list()

    for v_1 in next_tasks:
        # Detect cycles
        if v_1 in discovered:
            raise Exception(f"Cycle detected: found a back edge from {u_1} to {v_1}.")

        # Recurse into DFS tree
        if v_1 not in finished:
            node_visit(df_flat, v_1, discovered, finished)

    discovered.remove(u_1)
    finished.add(u_1)

    return discovered, finished

#################################################################################################
#   This Function warapper function to check for Cyclic from start of flow.
#   Means if the `task_depended_on` has multiple jobs, then `task_name` will have multiple rows
#   while traversing,If a node is already discovered then exception `Cyclic decteded`,else success.
#   This is a recusion function to find last node or already visited node.
##################################################################################################
def check_for_cyclic(df_flat):
    """cyclic dependency checks"""
    discovered = set()
    finished = set()

    starting_jobs = df_flat[df_flat['task_depended_on'] == '0']['task_name'].tolist()
    dependent_job=df_flat[df_flat['task_depended_on'] != '0']['task_name'].tolist()
    try:
        for u_1 in starting_jobs:
            if u_1 not in discovered and u_1 not in finished:
                discovered, finished = node_visit(df_flat, u_1, discovered, finished)

        for u_1 in dependent_job:
            if u_1 not in discovered and u_1 not in finished:
                discovered, finished = node_visit(df_flat, u_1 , discovered, finished)
    except Exception as exp:
        return "error",exp

    return "success", finished

##############################################################################################
#   This Function check for
#   Means if the `task_depended_on` has multiple jobs, then `task_name` will have multiple rows
#   while traversing, If a node is already discovered then exception `Cyclic decteded`,else success
#   This is a recusion function to find last node or already visited node.
###############################################################################################

def job_check(df_flat):
    'This function validates the struction for job and parent job structure.'
    v_err_status = 'success'
    v_err_msg = ''

    # Checking all parent jobs are subset of children jobs.
    set_childjob = set(df_flat['task_name'])
    set_parentjob = set(df_flat['task_depended_on'])

    if  '0' not in set_parentjob:
        v_err_status = 'failure'
        v_err_msg = 'Error: Entry point Job not found'
        return v_err_status, v_err_msg

    # Checking all parent jobs are subset of children jobs.
    set_parentjob.remove('0') # removing parent=0

    if set_parentjob.issubset(set_childjob) is False:
        v_err_status = 'failure'
        v_err_msg = 'Error: Depended on (parent task) should be part of Jobs (all task).'
        return v_err_status, v_err_msg

    # Check for cyclic dependency between jobs.
    v_error_status, v_err_msg = check_for_cyclic(df_flat)
    if v_error_status == 'error':
        v_err_status = 'failure'
        v_err_msg = 'Error: Cyclic job dependent in dataframe.'
        return v_err_status, v_err_msg

    return v_err_status, v_err_msg


def main_job(prj_nm,paths_data,pip_nm,run_id):
    """pipeline execution"""
    # Read the csv file
    pip_path= paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
    paths_data["pipeline_json_path"]+ pip_nm+ '.json'
    with open(pip_path,"r", encoding='utf-8') as jsonfile:
        json_data = json.load(jsonfile)
    df_3=pd.DataFrame(json_data['tasks_details'].items())
    df_return = df_flatten(df_3)
    # set(df_return['task_name'])
    status,msg=job_check(df_return)
    log1.info(status)
    log1.info(msg)
    if status=='failure':
        log1.error("Issue with the Pipeline json")
        # audit_json_path = paths_data["folder_path"] +paths_data["Program"]+\
        # prj_nm+paths_data["audit_path"]+pip_nm+'_audit_'+run_id+'.json'
        # audit(audit_json_path,json_data, pip_nm,run_id,'STATUS','FAILED')
        sys.exit()
    else:
        log1.info("reading pipeline json completed")

def audit(json_file_path,json_data, task_name,run_id,status,value):
    """ create audit json file and audits event records into it"""
    try:
        if path.isfile(json_file_path) is False:
            log1.info('audit started')
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
                # fcntl.flock(outfile, fcntl.LOCK_EX)
                outfile.write(json_object)
                # fcntl.flock(outfile, fcntl.LOCK_UN)
            # outfile.close()
            log1.info("audit json file created and audit done")
        else:
            with open(json_file_path, "r+", encoding='utf-8') as audit1:
                # fcntl.flock(outfile, fcntl.LOCK_EX)
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
                # fcntl.flock(outfile, fcntl.LOCK_UN)
                log1.info('Audit of an event has been made')
    except Exception as error:
        log1.exception("error in auditing json %s.", str(error))
        raise error

def copy(src_path,target_path):
    """ copies contents of one file to other file"""
    try:
        with open(src_path, "r", encoding='utf-8') as audit1:
            audit_data1 = json.load(audit1)
        # print(type(audit_data1))
        with open(target_path, "r+", encoding='utf-8') as audit2:
            audit_data2 = json.load(audit2)
            audit_data2.extend(audit_data1)
            audit2.seek(0)
            json.dump(audit_data2, audit2, indent=4, default=str)
    except Exception as error:
        log1.exception("error in copying json json %s.", str(error))
        raise error

def send_mail(message, prj_nm,paths_data,name,log_file_name):
    """Function to send emails for notyfying users on job status"""
    try:
        msg = MIMEMultipart()
        msg['From'] = paths_data["from_addr"]
        msg['To'] = paths_data["to_addr"]
        if message == "FAILED":
            msg['Subject'] = f"{name} execution Failed on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                       <p style="color:red;">The {name} in {prj_nm}'s project Execution Failed.</p>
                       <p>Thanks and Regards,</p>
                       <p>{paths_data["team_nm"]}</p>"""
        elif message == "STARTED":
            msg['Subject'] = f"{name} execution Started on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                       <p style="color:green;"> The {name} in {prj_nm}'s project has been Started.</p>
                       <p>Thanks and Regards,</p>
                       <p>{paths_data["team_nm"]}</p>"""
        elif message == "COMPLETED":
            msg['Subject'] = f"{name} execution Finished on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                       <p style="color:green;"> The {name} in {prj_nm}'s project  has been completed.</p>
                       <p>Thanks and Regards,</p>
                       <p>{paths_data["team_nm"]}</p>"""
        msg.attach(MIMEText(body, 'html'))
        if message != "STARTED":
            # path_log = f"/app/intel_new_1/+{prj_nm}+/ingestion_kart/Pipeline/logs/"
            path_log = paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
            paths_data["pipeline_log_path"]
            # log_file = "321_excel_to_csv_TaskLog_20230313142543_673.log"
            with open(path_log+log_file_name, "rb") as log_data:
                attachment = MIMEApplication(log_data.read(), _subtype="txt")
                attachment.add_header('Content-Disposition', 'attachment',
                                      filename=log_file_name)
                msg.attach(attachment)
        server = smtplib.SMTP(paths_data["EMAIL_SMTP"],paths_data["EMAIL_PORT"])
        server.starttls()
        text = msg.as_string()
        server.login(paths_data["email_user_name"],paths_data["email_password"])
        server.sendmail(paths_data["from_addr"], paths_data["to_addr"].split(','), text)
        log1.info('mail sent')
        server.quit()
    except Exception as error:
        log1.exception("Connection to mail server failed %s", str(error))
        raise error

def orchestrate_calling(prj_nm,paths_data,task_nm,pip_nm,run_id,log_file_name):
    """executes the orchestration at task or
    pipeline level based on the command given"""
# if __name__ == "__main__":
    global STATUS_LIST
    STATUS_LIST=[]
    try:
        if (str(pip_nm) == "-9999" ) or (task_nm != -9999 and pip_nm != -9999) or\
            (str(task_nm) != "-9999"):
            # log1.info("entered into task")
            df_1 = pd.DataFrame(columns=["task_name","task_depended_on","Job_Status"],index = [1])
            df_1['task_name']= task_nm
            df_1['task_depended_on']= 0
            df_1['Job_Status']= 'Start'
            # log1.info(df_1)
            file_path=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
            paths_data["status_txt_file_path"]+run_id+".txt"
            df_1.to_csv(file_path,mode='w', sep='\t',index = False, header=True)
        else:
            # log1.info("entered into pip")
            new_path = paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
            paths_data["pipeline_json_path"]+pip_nm+".json"
            # reading pipeline Json
            with open(r""+new_path,"r", encoding='utf-8') as jsonfile:
                json_data = json.load(jsonfile)
                #reading pipeline JSON completed
                # setting the audit josn path
                audit_json_path = paths_data["folder_path"] +paths_data["Program"]+\
                prj_nm+paths_data["audit_path"]+pip_nm+\
                '_audit_'+run_id+'.json'
                # log1.info(audit_json_path)
                audit(audit_json_path,json_data, pip_nm,run_id,'STATUS','STARTED')
            df_2=pd.DataFrame(json_data['tasks_details'].items())
            df_1 = df_flatten(df_2)
            df_1['Job_Status']= 'Start'
            file_path=paths_data["folder_path"]+paths_data["Program"]+prj_nm+\
            paths_data["status_txt_file_path"]+run_id+".txt"
            df_1.to_csv(file_path,mode='w', sep='\t',index = False, header=True)
        if task_nm != -9999 and pip_nm != -9999:
            log1.info("execution at task level")
            send_mail('STARTED', prj_nm,paths_data,task_nm,log_file_name)
            execute_job(prj_nm,paths_data,task_nm,pip_nm,run_id)
            # log1.info("executing the task")
        elif task_nm != -9999: #pip_nm == -9999
            log1.info("execution at task level")
            send_mail('STARTED', prj_nm,paths_data,task_nm,log_file_name)
            execute_job(prj_nm,paths_data,task_nm,str(pip_nm),run_id)
        elif task_nm == -9999 and pip_nm != -9999:
            log1.info("execution at pipeline level")
            send_mail('STARTED', prj_nm,paths_data,pip_nm,log_file_name)
            main_job(prj_nm,paths_data,pip_nm,run_id)
            STATUS_LIST=orchestration_execution(prj_nm,paths_data,pip_nm,run_id)
        else:
            log1.info("Please enter the correct command")
    except Exception as error:
        audit(audit_json_path,json_data, pip_nm,run_id,'STATUS','FAILED')
        send_mail("FAILED", prj_nm,paths_data,pip_nm,log_file_name)
        log1.exception("error in orchestrate_calling %s.", str(error))
        raise error
    finally:
        if (str(pip_nm) == "-9999" ) or (task_nm != -9999 and pip_nm != -9999) or \
        (str(task_nm) != "-9999"):
            # log1.info("***entered into task***")
            task_status_list = []
            df_2 =pd.read_csv(file_path, sep='\t')
            for i,row in df_2.iterrows():
                if row['task_depended_on']==0:
                    log1.info(row['task_name']+" task is "+row['Job_Status'])
                    task_status_list.append(row['Job_Status'])
            result = all(x == "SUCCESS" for x in task_status_list)
            if result is False:
                log1.info("Task %s Execution failed.",task_nm)
                send_mail("FAILED", prj_nm,paths_data,task_nm,log_file_name)
                log1.handlers.clear()
                # log1.shutdown()
            else:
                log1.info("Task %s Execution ended sucessfully.",task_nm)
                send_mail("COMPLETED", prj_nm,paths_data,task_nm,log_file_name)
                log1.handlers.clear()
        else:
            # log1.info("***entered into pip***")
            audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
            paths_data["audit_path"]+pip_nm+'_audit_'+run_id+'.json'
            if len(STATUS_LIST)==0:
                audit(audit_json_path,json_data, pip_nm,run_id,'STATUS',
                'FAILED DUE TO CYCLIC DEPENDENCY')
                log1.info("pipeline %s Execution FAILED DUE TO CYCLIC DEPENDENCY", pip_nm)
                send_mail("FAILED", prj_nm,paths_data,pip_nm,log_file_name)
                log1.handlers.clear()
            else:
                result = all(x == "SUCCESS" for x in STATUS_LIST)
                # log1.info(result)
                if result is False:
                    audit(audit_json_path,json_data, pip_nm,run_id,'STATUS','FAILED')
                    log1.info("pipeline %s Execution failed.", pip_nm)
                    send_mail("FAILED", prj_nm,paths_data,pip_nm,log_file_name)
                    log1.handlers.clear()
                else:
                    audit(audit_json_path,json_data, pip_nm,run_id,'STATUS','COMPLETED')
                    log1.info("pipeline %s Execution ended sucessfully.", pip_nm)
                    send_mail("COMPLETED", prj_nm,paths_data,pip_nm,log_file_name)
                    log1.handlers.clear()

                #combining the task jsons into pipeline jsons
                df_2 =pd.read_csv(file_path, sep='\t')
                task_list=[]
                for i,row in df_2.iterrows():
                    if (row['Job_Status'] == 'FAILED') or (row['Job_Status'] == 'SUCCESS'):
                        #log1.info(task_list)
                        task_list.append(row['task_name'])
                #task_list =df_2[df_2['Job_Status'] == 'FAILED'|'SUCCESS']['task_name'].to_list()
                #log1.info(task_list)
                # for j in list(set(task_list)):
                for j in list(set(task_list)):
                    copy(paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
                    paths_data["audit_path"]+j+'_audit_'+run_id+'.json',audit_json_path)
                    os.remove(paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
                    paths_data["audit_path"]+j+'_audit_'+run_id+'.json')
                if result is False:
                    sys.exit()

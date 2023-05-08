"""script for posting data into audit table"""
import json
import requests
from datetime import datetime

# try:
#     url = "http://localhost:8080/api/audit"
#     # json_file = "file.json"

#     # Read JSON data from file
#     with open(r"/app/intel_new_1/Program/ingestion_kart/Pipeline/Task/audit/P_222_audit_20230421115014_55.json",
#     "r", encoding='utf-8') as f:
#         json_data = json.load(f)
#     print(json_data)
#     # Send POST request with JSON payload
#     response = requests.post(url, json=json_data)
#     # response = requests.get(url)

#     # Here is an example json file u need to pass that file format or you
#     # can make it as a path and just take file name�

#     # Print response
#     print(response.status_code)
#     print(response.json())
# except Exception as error:
#     print("error in api_call %s.", str(error))
#     raise error

# try:
#     # audit_json_path = paths_data["folder_path"] +paths_data["Program"]+prj_nm+\
#     # paths_data["audit_path"]+task_or_pipeline+'_audit_'+run_id+'_'+iter_value+'.json'
#     # log1.info("audit_json_path: %s",audit_json_path)
#     # # Read JSON data from file
#     # with open(audit_json_path,"r", encoding='utf-8') as file:
#     #     json_data = json.load(file)
#     print("Proccess Started")
#     url = "http://localhost:8080/api/audit"
#     data = {
#         "pipeline_id": "12345678",
#         "TASK_OR_PIPE_LINE_NM": "task_name",
#         "run_id": "run_id",
#         "iteration": 1,
#         "audit_type": "status",
#         "audit_value": "value",
#         "process_dttm" : datetime.now()
#     }
#     # log1.info(json_data)
#     # Send POST request with JSON payload
#     # make the request with the dictionary as the request body
#     response = requests.post(url, data=data)

#     # print the response content
#     print(response.content)
#     print(response.status_code)
#     # Print response
#     print("response for audit json insert into database:%s", response.status_code)
#     print("inserted audit json into db")
# except Exception as error:
#     print("error in api_call %s.", str(error))
#     raise error
url = "http://localhost:8080/api/audit"

data = {
    "pipeline_id": "12345678",
    "task/pipeline_name": "task_name",
    "run_id": "run_id",
    "iteration": 1,
    "audit_type": "status",
    "audit_value": "value",
    "process_dttm" : datetime.now().strftime('%Y-%m-%d %H:%M:%S') # format the datetime object as a string
}

response = requests.post(url, json=data) # send the data in the request body using the `json` parameter
print(data)
print(response.content)
print(response.status_code)

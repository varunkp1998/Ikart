# """ script for writing data to parquet"""
import logging
import os
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from utility import replace_date_placeholders

task_logger = logging.getLogger('task_logger')

def write(json_data: dict, dataframe, counter) -> bool:
    """ function for writing to parquet """
    try:
        target = json_data["task"]["target"]
        file_path = target["file_path"]
        file_name = target["file_name"]
        file_name = replace_date_placeholders(target['file_name'])
        task_logger.info("converting data to parquet initiated")
        check = True if target['index'] == "False" else False
        # Reset the index and create an 'index' column
        dataframe.reset_index(drop=check, inplace=True)

        if counter ==1: # If it's the first chunk, write the data to a new Parquet file
            if os.path.exists(target["file_path"]+file_name):
                os.remove(target["file_path"]+file_name)
            if target["audit_columns"] == "active":
                # if audit_columns are active
                dataframe['CRTD_BY']="etl_user"
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                table = pa.Table.from_pandas(dataframe)
                pq.write_table(table, file_path + file_name, version='1.0')
            else:
                table = pa.Table.from_pandas(dataframe)
                pq.write_table(table, file_path + file_name, version='1.0')
        else: # If it's not the first chunk, read the existing Parquet file and append the new data
            if target["audit_columns"] == "active":
                # if audit_columns are active
                dataframe['CRTD_BY']="etl_user"
                dataframe['CRTD_DTTM']= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dataframe['UPDT_BY']= " "
                dataframe['UPDT_DTTM']= " "
                table = pa.Table.from_pandas(dataframe)
                existing_table = pq.read_table(file_path + file_name)
                updated_table = pa.concat_tables([existing_table, table])
                pq.write_table(updated_table, file_path + file_name, version='1.0')
            else:
                table = pa.Table.from_pandas(dataframe)
                existing_table = pq.read_table(file_path + file_name)
                updated_table = pa.concat_tables([existing_table, table])
                pq.write_table(updated_table, file_path + file_name, version='1.0')
        task_logger.info("parquet conversion completed")
        return True
    except Exception as error:
        task_logger.exception("converting_to_parquet() is %s", str(error))
        raise error

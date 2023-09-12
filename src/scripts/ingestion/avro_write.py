import logging
import fastavro
import io
import pandas as pd

task_logger = logging.getLogger('task_logger')

def write(json_data: dict, dataframe, counter) -> bool:
    """ function for writing to Avro """
    try:
        file_path = json_data["task"]["target"]["file_path"]
        file_name = json_data["task"]["target"]["file_name"]

        task_logger.info("converting data to Avro initiated")

        if counter == 1:
            # If it's the first chunk, write the data to a new Avro file
            with open(file_path + file_name, 'wb') as avro_file:
                fastavro.writer(avro_file, dataframe.to_dict(orient='records'))
        else:
            # If it's not the first chunk, read the existing Avro file and append the new data
            with open(file_path + file_name, 'rb') as avro_file:
                existing_records = list(fastavro.reader(avro_file))
            updated_records = existing_records + dataframe.to_dict(orient='records')
            with open(file_path + file_name, 'wb') as avro_file:
                fastavro.writer(avro_file, updated_records)

        task_logger.info("Avro conversion completed")
        return True
    except Exception as error:
        task_logger.exception("converting_to_avro() is %s", str(error))
        raise error

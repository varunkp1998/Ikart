""" script for writing data to parquet"""
import logging

log2 = logging.getLogger('log2')

def write(json_data: dict, datafram ) -> bool:
    """ function for writing to  parquet"""
    try:
        log2.info("converting data to parquet initiated")
        # df=pd.DataFrame(index=file.index).reset_index().astype(str)
        # frames = [df, file]
        # datafram = pd.concat(frames)
        datafram.astype(str).to_parquet(json_data["task"]["target"]["target_file_path"] + \
        json_data["task"]["target"]["target_file_name"],engine='auto',
                index=False)
        # df.columns = df.columns.astype(str)
        log2.info(" parquet conversion completed")
        return True
    except Exception as error:
        log2.exception("converting_to_parquet() is %s", str(error))
        raise Exception("converting_to_parquet(): " + str(error)) from error

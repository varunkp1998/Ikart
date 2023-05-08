'''Importing Modules'''
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

EMAIL_SMTP = "smtp.gmail.com"
EMAIL_PORT = 587
email_user_name = "Ikart1927@gmail.com"
email_password = "otqvwijdyvommrcm"
from_addr = "Ikart1927@gmail.com"
to_addr = "ingestionkart@gmail.com,pp552081@gmail.com"
cc_addr = " "
team_nm = "Ingestion Team"

import pandas as pd

def table(start_time, end_time):
    """table structure"""
    table_dict = {'S.NO': [1], 'JOB_NAME': ['P_222'], 'JOB_TYPE': ['INGESTION'],
                  'SOURCE_COUNT': ['NA'], 'TARGET_COUNT': ['NA'], 'START_TIME': [start_time],
                  'END_TIME': [end_time], 'STATUS': 'COMPLETED'}
    tbl_format = pd.DataFrame(table_dict)
    return tbl_format

x = table('12-12-2023', '12-12-2023')

# Generate HTML code
html_code = x.to_html(index=False)
styled_table = x.style#it genetrate css style

# Define a function that takes a value and returns the corresponding CSS color
def colorize_status(status):
    '''function which takes a value and returns the corresponding css color'''
    if status == 'COMPLETED':
        return 'green'
    elif status == 'FAILED':
        return 'red'
# Apply the colorize_status function to the 'STATUS' column and set the CSS color property
# styled_table1 = styled_table.apply(lambda row: ['background-color: {}'.format(colorize_status(val))
#                                                for val in row], axis=1, subset=['STATUS'])
# to generate the HTML code with the CSS styling
styled_html_code = styled_table.apply(lambda row: ['background-color: {}'.format(colorize_status(val))
                                               for val in row], axis=1, subset=['STATUS']).hide_index().render()
# Define a CSS style string to set the background color of the table header to green
header_style = '<style>th { background-color: green; }</style>'
# Add the header_style to the beginning of the HTML code
styled_html_code = header_style + styled_html_code
# Print the final HTML code with the CSS styling applied
print(styled_html_code)


def send_start_mail(message, arg_prj_nm):
    """Function to send emails for notifying users on job status"""
    try:
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Cc'] = cc_addr
        if message == "Failed":
            msg['Subject'] = f"speaker_extract_job Failed on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                        <p>The task/pipeline Execution Failed.</p>
                        <p>Thanks and Regards,</p><p>{team_nm}</p>"""
        elif message == "started":
            msg['Subject'] = f"Task/pipeline execution Started on {str(datetime.now())}"
            # body = f"""<p>Hi Team,</p>
            #             <p style="color:green;"> The {arg_prj_nm} project Task/pipeline has been Started.</p>
            #             <p>Thanks and Regards,</p>
            #             <p>{team_nm}</p>"""
            body = """
                    <p>Hi Team,</p>
                    <p style="color:green;">The following table contains the status of the job:</p>
                    <table border="1" cellpadding="5">
                        <tr>
                            <th>Job Name</th>
                            <th>Status</th>
                            <th>Start Time</th>
                            <th>End Time</th>
                        </tr>
                        <tr>
                            <td>Job 1</td>
                            <td>Success</td>
                            <td>2023-04-20 10:00:00</td>
                            <td>2023-04-20 11:00:00</td>
                        </tr>
                        <tr>
                            <td>Job 2</td>
                            <td>Failed</td>
                            <td>2023-04-20 11:00:00</td>
                            <td>2023-04-20 11:30:00</td>
                        </tr>
                    </table>
                    <p>Thanks and Regards,</p>
                    <p>{team_nm}</p>
                    """

        else:
            msg['Subject'] = f"Task/pipeline execution Finished on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                        <p style="color:green;"> The {arg_prj_nm} project Task/pipeline has been completed.</p>
                        <p>Thanks and Regards,</p>
                        <p>{team_nm}</p>"""
        msg.attach(MIMEText(body, 'html'))
        if message != "started":
            Path = "/app/intel_new_1/Program/ingestion_kart/Pipeline/logs/"
            LOG_FILE = "1235_csv_to_parquet_TaskLog_20230412084015_248.log"
            with open(Path+LOG_FILE, "rb") as log_data:
                attachment = MIMEApplication(log_data.read(), _subtype="txt")
                attachment.add_header('Content-Disposition', 'attachment',
                                      filename=LOG_FILE)
                msg.attach(attachment)
        server = smtplib.SMTP(EMAIL_SMTP, EMAIL_PORT)
        server.starttls()
        text = msg.as_string()
        server.login(email_user_name, email_password)
        server.sendmail(from_addr, to_addr.split(',') + cc_addr.split(','), text)  # fix here
        print('mail sent')
        server.quit()
    except Exception as err:
        print("Connection to mail server failed %s", str(err))
        raise err


def send_mail(message, prj_nm,paths_data,name,log_file_path,log_file_name):
    """Function to send emails for notyfying users on job status"""
    try:
        msg = MIMEMultipart()
        msg['From'] = paths_data["from_addr"]
        msg['To'] = paths_data["to_addr"]
        msg['Cc'] = paths_data["cc_addr"]
        if message == "FAILED":
            msg['Subject'] = f"FAILURE: IKART: {name} execution Failed on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                       <p style="color:red;">The {name} in {prj_nm}'s project Execution Failed.</p>
                       <p>Thanks and Regards,</p>
                       <p>{paths_data["team_nm"]}</p>
                       """
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
                       <p>{paths_data["team_nm"]}</p>
                       """
        msg.attach(MIMEText(body, 'html'))
        if message != "STARTED":
            with open(log_file_path+log_file_name, "rb") as log_data:
                attachment = MIMEApplication(log_data.read(), _subtype="txt")
                attachment.add_header('Content-Disposition', 'attachment',
                                      filename=log_file_name)
                msg.attach(attachment)
        server = smtplib.SMTP(paths_data["EMAIL_SMTP"],paths_data["EMAIL_PORT"])
        server.starttls()
        text = msg.as_string()
        server.login(paths_data["email_user_name"],paths_data["email_password"])
        server.sendmail(paths_data["from_addr"], paths_data["to_addr"].split(','),paths_data["cc_addr"].split(','), text)
        log1.info('mail sent')
        server.quit()
    except Exception as error:
        log1.exception("Connection to mail server failed %s", str(error))
        raise error

send_start_mail("started", "Intellikart")
# send_start_mail("Failed", "Intellikart")
# send_start_mail("sucess", "Intellikart")


# send_mail("started", " " , "Intellikart")
# send_mail("Failed", "due to cyclic dependency" , "Intellikart")
# send_mail("sucess", " " , "Intellikart")
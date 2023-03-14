import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

EMAIL_SMTP = "smtp.gmail.com"
EMAIL_PORT = 587
email_user_name = "ravikiran1927@gmail.com"
email_password = "qzfclssttgahabjx"
from_addr = "ravikiran1927@gmail.com"
to_addr = "ingestionkart@gmail.com,pp552081@gmail.com"
team_nm = "Ingestion Team"

def send_start_mail(message, arg_prj_nm):
    """Function to send emails for notyfying users on job status"""
    try:
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        if message == "Failed":
            msg['Subject'] = f"speaker_extract_job Failed on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>
                        <p>The task/pipeline Execution Failed.</p>            <p>Thanks and Regards,</p><p>{team_nm}</p>"""
        elif message == "started":
            msg['Subject'] = f"Task/pipeline execution Started on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>            <p style="color:green;"> The {arg_prj_nm} project Task/pipeline has been Started.</p>            <p>Thanks and Regards,</p>            <p>{team_nm}</p>"""
            # body = f" Hi Team, \n\n The {par_proj_name} project script has been Started. \n\n Thanks and Regards \n {team_nm}"
        else:
            msg['Subject'] = f"Task/pipeline execution Finished on {str(datetime.now())}"
            body = f"""<p>Hi Team,</p>            <p style="color:green;"> The {arg_prj_nm} project Task/pipeline has been completed.</p>            <p>Thanks and Regards,</p>            <p>{team_nm}</p>"""
        msg.attach(MIMEText(body, 'html'))
        if message != "started":
            Path = "/app/intel_new_1/Program/ingestion_kart/Pipeline/logs/"
            LOG_FILE = "321_excel_to_csv_TaskLog_20230313142543_673.log"
            with open(Path+LOG_FILE, "rb") as log_data:
                attachment = MIMEApplication(log_data.read(), _subtype="txt")
                attachment.add_header('Content-Disposition', 'attachment',
                                      filename=LOG_FILE)
                msg.attach(attachment)
        server = smtplib.SMTP(EMAIL_SMTP,EMAIL_PORT)
        server.starttls()
        text = msg.as_string()
        server.login(email_user_name,email_password)
        server.sendmail(from_addr, to_addr.split(','), text)
        print('mail sent')
        server.quit()
    except Exception as e:
        print("Connection to mail server failed %s", str(e))
        raise Exception
    
# send_start_mail("started", "Intellikart")
send_start_mail("Failed", "Intellikart")
# send_start_mail("sucess", "Intellikart")


# send_mail("started", " " , "Intellikart")
# send_mail("Failed", "due to cyclic dependency" , "Intellikart")
# send_mail("sucess", " " , "Intellikart")
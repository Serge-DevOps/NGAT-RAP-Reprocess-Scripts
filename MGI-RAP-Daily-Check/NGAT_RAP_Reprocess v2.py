import os
import pandas as pd
import requests
import logging
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging to write to 'output.log' and the console
current_date = datetime.now().strftime("%Y-%m-%d")
log_file_path = f'output_{current_date}.log'
logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO to capture standard output
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    handlers=[
        logging.FileHandler(log_file_path),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

optional_csv_file_path = os.getenv('optional_csv_file') #default is None - Optional - Use a local CSV instead of the Sentry API Query
if (optional_csv_file_path == ''):
    optional_csv_file_path = None

sentry_api_interval = os.getenv('sentry_api_interval')
if (sentry_api_interval == ''):
    sentry_api_interval = '24h' #1h, 24h, 7d, 14d, 30d, 60d, 90d - Default is 24h - Time period for the Sentry API Query

sentry_api_list_url = 'https://sentry.io/api/0/organizations/mhs-sentry/events/?field=id&field=assessmentId&query=(url:[https://a2.mhs.com/ngat/Assessment/Complete,https://a2.mhs.com/ngat/Assessment/Timeout]%20AND%20issue:RS-RAP-NGAT-2C)&statsPeriod={statsPeriod}&cursor=0:{cursor}:0'
sentry_api_info_url = 'https://sentry.io/api/0/projects/mhs-sentry/rs-rap-ngat/events/{event_id}/attachments/?download=1'
sentry_api_data_url = 'https://sentry.io/api/0/projects/mhs-sentry/rs-rap-ngat/events/{event_id}/attachments/{attachment_id}/?download=1'
rap_save_url = 'https://a2.mhs.com/rap_api/api/Assessment/Save'
rap_get_url = 'https://a2.mhs.com/rap_api/api/Assessment/GetResult/ByAssessmentId?assessmentId={assessment_id}&cleansed=true'
rap_sb_url = 'https://a2.mhs.com/rap_api/api/Assessment/ResendToAzureServiceBus/ByAssessmentId'
mgi_get_url = 'https://prod-70.eastus.logic.azure.com/workflows/84af5901dfff43648b033ff03e783787/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=3Go-NLV-jGBdP-4Ort09YDUYdiuj8QsXOxrWCJMKBKg&sessionid={sessionid}&versionid={versionid}'

# Authentication token and API key from .env file
auth_token = os.getenv('auth_token')
api_key = os.getenv('api_key')
api_resend_pw = os.getenv('api_resend_pw')

df = pd.DataFrame(columns=['assessmentId', 'id', 'project.name'])
        
def reprocess_records(file_path):
    global df
    total_records = 0

    try:
        if (file_path != None):
            logging.info(f"Loading records from CSV file: {file_path}")    
            # Load the custom CSV into Dataframe
            df = pd.read_csv(file_path)        
        else:
            logging.info(f"Loading records from Sentry API")  
            # Load the Senty Events for the last 24 hrs into Dataframe
            get_sentry_events(0,sentry_api_interval)
        
        total_records = len(df)
        logging.info(f"Total # of Records to Process: { total_records }")        
       
        # Loop through each row in the DataFrame
        for index, row in df.iterrows():
            
            event_id = row['id']
            assessmentId = row['assessmentId']

            logging.info(f"{ index } - {assessmentId} - Start Processing Record----------------------------------")
            
            ngat_rap_data = get_from_rap(index, assessmentId)
            
            if ngat_rap_data:
                if ((ngat_rap_data['status'] == 'Complete') or (ngat_rap_data['status'] == 'Fail')): #If Status of the RAP DB is Complete OR Fail and it is not in MGI > Resend to Service Bus                        
                    sessionId = ngat_rap_data["sessionId"]
                    versionId = ngat_rap_data["versionID"]
                    studentId = ngat_rap_data["userData"]['studentID']
                    studentName = ngat_rap_data["userData"]['studentName']
                    platformId = ngat_rap_data["userData"]['platformID']

                    #Check MGI to see if data is missing
                    found_mgi = get_from_mgi(index, assessmentId,sessionId,versionId)

                    if (found_mgi == False):
                        #Resend to Service Bus only if missing from MGI
                        send_to_sb(index, assessmentId, studentId, studentName, platformId)
                    else:
                         logging.info(f"{ index } - {assessmentId} - NGAT Record is already stored in MGI. AssessmentID: {assessmentId} StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId} -- No action needed --")

            if ((ngat_rap_data == None) or (ngat_rap_data != None and ngat_rap_data['status'] == 'InProgress')):  #If the Asssessment ID is NOT FOUND or it is "In Progress", means RAP does not have the Data or doesn't have the FINAL data in its DB > Get Attachment and POST to RAP               
                
                attachment_info = get_attachment_info(index, assessmentId, event_id)

                if not attachment_info:
                    logging.warning(f"{ index } - {assessmentId} - NGAT Record does not have Attachment Data on Sentry -- Not possible to Recover --")
                else:
                    for attachment in attachment_info:
                        attachment_id = attachment.get('id')
                        if attachment_id:
                            attachment_data = get_attachment_data(index, assessmentId, event_id, attachment_id)
                            if attachment_data:                               
                                studentId = attachment_data["userData"]['studentID']
                                studentName = attachment_data["userData"]['studentName']
                                platformId = attachment_data["userData"]['platformID']
                                wrapped_data = wrap_attachment_data(attachment_data)
                                send_to_rap(index, wrapped_data, assessmentId, studentId, studentName, platformId)
                
            logging.info(f"{ index } - {assessmentId} - Completed Processing Record---------------------------------")            
        
    except Exception as e:
        logging.critical(f"Failed to Re-process NGAT Records. Please re-run the script: {e}")
        return total_records         
    return total_records

def get_sentry_events(cursor,statsPeriod):
    global df

    url = sentry_api_list_url.replace('{cursor}', str(cursor)).replace('{statsPeriod}', str(statsPeriod))
    headers = {'Authorization': f'Bearer {auth_token}'}
    #get the first page (cursor = 0)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            #add to results List
            page_df = pd.DataFrame(response.json()['data'])
            df = pd.concat([df, page_df], ignore_index=True)

            #check link header for results=True
            has_next = response.links['next']['results']
            #if true, request again with cursor+100
            if (has_next == 'true'):
                get_sentry_events(cursor+100,statsPeriod)
        else:
            logging.error(f"Failed to get Sentry Event List. Status: {response.status_code}. URL: {url}")
    except requests.RequestException as e:
        logging.error(f"Request Failed for Sentry Event List: {e} -- Retrying --") 
        get_sentry_events(cursor,statsPeriod)
               
         

# Function to get attachment info from Sentry API
def get_attachment_info(index, assessment_id, event_id):
    url = sentry_api_info_url.replace('{event_id}', event_id)
    headers = {'Authorization': f'Bearer {auth_token}'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"{index} - {assessment_id} - Failed to get attachment info for EventId {event_id}. Status: {response.status_code}. URL: {url}")
            return None
    except requests.RequestException as e:
        logging.error(f"{index} - {assessment_id} - Request Failed for EventId {event_id}: {e}")
        return None
    
# Function to get attachment data from Sentry API
def get_attachment_data(index, assessment_id, event_id, attachment_id):
    url = sentry_api_data_url.replace('{event_id}', event_id).replace('{attachment_id}', attachment_id)
    headers = {'Authorization': f'Bearer {auth_token}'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            if 'application/json' in response.headers.get('Content-Type'):
                return response.json()
            else:
                return {'event_id': event_id, 'attachment_id': attachment_id, 'binary_data': response.content.hex()}
        else:
            logging.error(f"{index} - {assessment_id} - Failed to get attachment data for EventId {event_id}, AttachmentId {attachment_id}. Status: {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"{index} - {assessment_id} - Request Failed for EventId {event_id}, AttachmentId {attachment_id}: {e}")
        return None

# Function to determine test type from attachment data
def determine_test_type(attachment_data):
    tests = attachment_data.get('userData', {}).get('tests', [])
    for test in tests:
        if test.get('isCompleted') == 1:
            return test.get('fullName')
    logging.critical(f"AssessType: Unknown")
    return "Unknown"

# Function to wrap attachment data for RAP API
def wrap_attachment_data(attachment_data):
    test_type = determine_test_type(attachment_data)
    return {"params": attachment_data, "testToUpdate": test_type}


# Function to send attachment data to RAP API
def send_to_rap(index, wrapped_data, assessment_id, studentId, studentName, platformId):
    headers = {'Apikey': api_key, 'Content-Type': 'application/json'}
    try:        
        response = requests.post(rap_save_url, headers=headers, json=wrapped_data)
        if response.status_code == 200:
            logging.warning(f"{index} - {assessment_id} - {response.status_code}: NGAT is reprocessed. AssessmentID: {assessment_id} StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId} Test: {wrapped_data['testToUpdate']}")
        else:
            logging.error(f"{index} - {assessment_id} - {response.status_code}: NGAT can't be processed. AssessmentID: {assessment_id} StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId} Response: {response.text}")
    except requests.RequestException as e:
        logging.error(f"{index} - {assessment_id} - StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId} Failed saving NGAT assessment to RAP API: {e}")


# Function to trigger a Service Bus request
def send_to_sb(index, assessment_id, studentId, studentName, platformId):
        headers = {'Apikey': api_key, 'Content-Type': 'application/json'}
        try:
            wrapped_data = { "password": api_resend_pw, "assessmentId" : assessment_id }
        
            response = requests.post(rap_sb_url, headers=headers, json=json.dumps(wrapped_data))
            if response.status_code == 200:
                logging.warning(f"{index} - {assessment_id} - {response.status_code}: NGAT is re-sent to Service Bus. AssessmentID: {assessment_id} StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId}")
            else:
                logging.error(f"{index} - {assessment_id} - {response.status_code}: NGAT can't be re-sent to Service Bus. AssessmentID: {assessment_id} StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId} Response: {response.text}")
        except requests.RequestException as e:
            logging.error(f"{index} - {assessment_id} - StudentID: {studentId} StudentName: {studentName} PlatformID: {platformId} Failed Re-sending to Service Bus: {e}")


def get_from_rap(index, assessment_id):
    headers = {'Apikey': api_key, 'Content-Type': 'application/json', 'Accept': 'application/json'}
    try:
        url = rap_get_url.replace('{assessment_id}', str(assessment_id))
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()['params']
        else:            
            logging.error(f"{index} - {assessment_id} - {response.status_code}: Failed to Load from RAP API. Response: {response.text}")
            return None
    except requests.RequestException as e:
        logging.error(f"{index} - {assessment_id} - Failed Getting NGAT assessment from RAP API: {e}")


def get_from_mgi(index, assessment_id, sessionId, versionId):
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    try:
        url = mgi_get_url.replace('{sessionid}', sessionId).replace('{versionid}', str(versionId))
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()['found']
        else:            
            logging.error(f"{index} - {assessment_id} - {response.status_code}: Failed to Load from MGI. Response: {response.text}")
            return False
    except requests.RequestException as e:
        logging.error(f"{index} - {assessment_id} - Failed Getting NGAT assessment from MGI: {e}")
        return False

def send_log_email(total_records, attachment_path):
    try:
        # Create a multipart message
        msg = MIMEMultipart()
        msg['From'] = os.getenv('sender_email')

        msg['To'] = os.getenv('receiver_email')

        if (os.getenv('cc_email') != ''):
            msg['Cc'] = os.getenv('cc_email')

        msg['Subject'] = f'** MGI/NGAT - {total_records} Records processed for {current_date}'

        to_addresses = os.getenv('receiver_email').split(',')
        cc_addresses = []

        if (os.getenv('cc_email') != ''):
            cc_addresses = os.getenv('cc_email').split(',')

        body = f'{total_records} Total Processed Records.\n\nAttached is the NGAT Re-processing Log for {current_date}.\n\nAlert sent from PrdApp4 D:\\Automation_Scripts\\MGI-RAP-Daily-Check'
        # Attach the body message
        msg.attach(MIMEText(body, 'plain'))

        # Open the file to be attached
        filename = os.path.basename(attachment_path)
        attachment = open(attachment_path, "rb")

        # Create MIMEBase object and encode the attachment
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)

        # Add header with the filename
        part.add_header('Content-Disposition', f'attachment; filename= {filename}')

        # Attach the file to the message
        msg.attach(part)

        # Close the file
        attachment.close()

        recipients = to_addresses + cc_addresses

        # Connect to the SMTP server
        with smtplib.SMTP(os.getenv('smtp_server')) as server:
           # Send the email
           server.sendmail(os.getenv('sender_email'), recipients, msg.as_string())

        logging.info(f"Log E-mail sent successfully!")
    except Exception as e:
        logging.critical(f"Failed to send Log E-mail. Records have safely been processed: {e}")

total_records = reprocess_records(optional_csv_file_path)

if (os.getenv('smtp_server') != ''):
    send_log_email(total_records, log_file_path)
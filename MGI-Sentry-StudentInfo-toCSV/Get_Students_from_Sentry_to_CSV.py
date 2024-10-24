import os
import csv # to write into a CSV file
import pandas as pd
import requests
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import datetime

# Load environment variables
load_dotenv()


# CSV files path ##################################################################################################################
ngat_requests_csv_file_path = 'NGAT_Sept1-30.csv' # Replace with Requests CSV File that contains Sentry Event IDs
###################################################################################################################################


api_base_url_info = 'https://sentry.io/api/0/projects/mhs-sentry/rs-rap-ngat/events/{event_id}/attachments/?download=1'
api_base_url_data = 'https://sentry.io/api/0/projects/mhs-sentry/rs-rap-ngat/events/{event_id}/attachments/{attachment_id}/?download=1'


# Authentication token and API key from .env file
auth_token = os.getenv('auth_token')
api_key = os.getenv('api_key')
api_resend_pw = os.getenv('api_resend_pw')


# Create the "output" directory if it doesn't exist
log_directory = os.path.join(os.getcwd(), 'outputCSV')
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    
    

def load_and_loop_csv(file_path: str):
    # Define the CSV file path with the results
    output_csv_file = os.path.join(log_directory, f"Sentry_Search_{ngat_requests_csv_file_path.replace(' ', '_').replace('.csv', '')}.csv")


    # Open the CSV output file within the function.
    try:
        with open(output_csv_file, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the header row to the CSV file
            csv_writer.writerow(["dateSubmitted", "platformID", "studentName", "studentId", "status", "testType", "assessmentId", "event_id"])
        
            # Load the CSV file into a pandas DataFrame
            df = pd.read_csv(file_path)
            
            # Loop through each row in the DataFrame
            for index, row in df.iterrows():        
                event_id = row['id']  # Add event_id
                if event_id and isinstance(event_id, str) and len(event_id) > 10:
                    attachment_info = get_attachment_info(event_id)
                    if not attachment_info:
                        continue

                    for attachment in attachment_info:
                        attachment_id = attachment.get('id')
                        if attachment_id:
                            attachment_data = get_attachment_data(event_id, attachment_id)
                            if attachment_data:
                                dateSubmitted = str(attachment_data['dateTimeSubmitted']) if pd.notna(attachment_data['dateTimeSubmitted']) else 'N/A' # Convert dateSubmitted to string and handle NaN values
                                platformID = attachment_data['userData'].get('platformID')
                                studentName = attachment_data['userData'].get('studentName').upper() if attachment_data['userData'].get('studentName') else None
                                studentId = attachment_data['userData'].get('studentID')
                                status = attachment_data['status']
                                test_type = determine_test_type(attachment_data)
                                assessmentId = attachment_data['assessmentId']                

                                # Write the data into the CSV file, including event_id
                                if studentName:
                                    csv_writer.writerow([dateSubmitted, platformID, studentName, studentId, status, test_type, assessmentId, event_id])
                                else:
                                    print("Student name not found")

    except Exception as e:
        logging.error(f"Error processing CSV: {e}")

def check_assessment_id_in_csv(file_path: str, assessment_id: str) -> bool:

    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Check if 'assessmentId' column exists
        if 'assessmentId' not in df.columns:
            raise ValueError("The CSV file does not contain an 'assessmentId' column.")
        
        # Check if the given assessment ID exists in the column
        return assessment_id in df['assessmentId'].values

    except Exception as e:
        print(f"Error: {e}")
        return False
    
# Function to get attachment info from Sentry API
def get_attachment_info(event_id):
    url = api_base_url_info.replace('{event_id}', event_id)
    headers = {'Authorization': f'Bearer {auth_token}'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to get attachment info for EventId {event_id}. Status: {response.status_code}. URL: {url}")
            return None
    except requests.RequestException as e:
        logging.error(f"Request error for EventId {event_id}: {e}")
        return None
    
# Function to get attachment data from Sentry API
def get_attachment_data(event_id, attachment_id):
    url = api_base_url_data.replace('{event_id}', event_id).replace('{attachment_id}', attachment_id)
    headers = {'Authorization': f'Bearer {auth_token}'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            if 'application/json' in response.headers.get('Content-Type'):
                return response.json()
            else:
                return {'event_id': event_id, 'attachment_id': attachment_id, 'binary_data': response.content.hex()}
        else:
            logging.error(f"Failed to get attachment data for EventId {event_id}, AttachmentId {attachment_id}. Status: {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"Request error for EventId {event_id}, AttachmentId {attachment_id}: {e}")
        return None

# Function to determine test type from attachment data
def determine_test_type(attachment_data):
    tests = attachment_data.get('userData', {}).get('tests', [])
    for test in tests:
        if test.get('isCompleted') == 1:
            return test.get('fullName')
    logging.critical(f"AssessType: Unknown")
    return "Unknown"

load_and_loop_csv(ngat_requests_csv_file_path)
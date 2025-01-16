import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Initialize Boto3 clients
s3_client = boto3.client('s3')
ses_client = boto3.client('ses')  # Specify the AWS region from environment variable

# Email settings from environment variables
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

# S3 and processing configuration
BUCKET_NAME = os.getenv('BUCKET_NAME')
FOLDER_PREFIX = os.getenv('FOLDER_PREFIX')  # 'Adverse_reaction_reports/report_details_output_'

def fetch_s3_file(bucket_name, file_key):
    """Fetches JSON file from S3 bucket and parses it."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        body_content = response['Body'].read().decode('utf-8')

        if not body_content:
            print(f"Warning: Empty content retrieved from {file_key}.")
            return None

        return json.loads(body_content)
    except ClientError as e:
        print(f"Error fetching the file: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}. Content: {body_content}")
        return None

def generate_email_body(data, sent_date):
    """Generates HTML email body with a single table including all entries."""
    html_body = f"""
    <html>
    <body>
        <h2 style="color: black; text-align: center; font-size: 24px;">Adverse Reaction Report - Alert</h2>
        <p style="color: black;">This email contains the results from the extraction of Adverse Reaction Report.</p>
        <p style="color: black;">The alert results cover the screening period up to <strong>{sent_date}</strong>.</p>
        <table border="1" cellpadding="5" cellspacing="0">
            <tr>
                <th style="color: black;">Sl.No</th>
                <th style="color: black;">Adverse Reaction Report Number</th>
                <th style="color: black;">Market Authorization Holder AER Number</th>
                <th style="color: black;">Initial Received Date</th>
                <th style="color: black;">Source of Report</th>
                <th style="color: black;">Age</th>
                <th style="color: black;">Gender</th>
                <th style="color: black;">Suspected Product Brand Name</th>
                <th style="color: black; width: 300px; padding-left: 10px; padding-right: 10px;">Adverse Reaction Terms</th>  <!-- Increased width and padding -->
            </tr>
    """

    # Add each report as a row
    for idx, report in enumerate(data, start=1):
        # formatted_reactions = ', '.join(part.strip().replace(' ', '') for part in report['pt_name_eng'].split(','))       
        html_body += f"""
            <tr>
                <td style="color: black;">{idx}</td>
                <td style="color: black;">{report['report_no']}</td>
                <td style="color: black;">{report.get('mah_no', 'N/A')}</td>
                <td style="color: black;">{report['datintreceived']}</td>
                <td style="color: black;">{report['source_eng']}</td>
                <td style="color: black;">{report['age']} {report['age_unit_eng']}</td>
                <td style="color: black;">{report['gender_eng']}</td>
                <td style="color: black;">{report['drug_name']}</td>
                <td style="color: black;">{report['pt_name_eng']}</td>
            </tr>
        """

    html_body += """
        </table>
    </body>
    </html>
    """
    return html_body

def send_email(subject, body_html):
    """Sends an email using SES."""
    try:
        response = ses_client.send_email(
            Source=SENDER_EMAIL,
            Destination={
                'ToAddresses': [RECIPIENT_EMAIL],
            },
            Message={
                'Subject': {'Data': subject},
                'Body': {'Html': {'Data': body_html}}
            }
        )
        print(f"Email sent! Message ID: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending email: {e}")

def get_latest_file(bucket_name, folder_prefix):
    """Fetches the most recent file based on LastModified date from the specified folder in S3."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        files = response.get('Contents', [])

        # Sort files by last modified date and return the key of the most recent file
        files.sort(key=lambda x: x['LastModified'], reverse=True)
        if files:
            return files[0]['Key']
        return None
    except ClientError as e:
        print(f"Error fetching the file list: {e}")
        return None

def lambda_handler(event, context):
    """Main Lambda handler."""
    latest_file = get_latest_file(BUCKET_NAME, FOLDER_PREFIX)
    if not latest_file:
        print("No files found in the specified folder.")
        return {'statusCode': 200, 'body': 'No files found in the specified folder.'}

    data = fetch_s3_file(BUCKET_NAME, latest_file)
    if not data:
        print(f"Error retrieving or decoding content from {latest_file}.")
        return {'statusCode': 200, 'body': f"Error retrieving or decoding content from {latest_file}."}

    sent_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    email_body = generate_email_body(data, sent_date)
    subject = f"Adverse Reaction Alert - {sent_date}"
    
    send_email(subject, email_body)

    return {'statusCode': 200, 'body': 'Email sent successfully.'}

import json
import boto3
import pdfkit
import PyPDF2
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import time
from datetime import datetime
import logging
import os
# Initialize the S3 client
s3_client = boto3.client('s3')

def get_latest_file_from_s3(bucket_name, prefix):
    """
    Retrieve the latest file from a specific directory in the S3 bucket.

    :param bucket_name: The name of the S3 bucket
    :param prefix: The directory or folder within the bucket
    :return: The key of the most recently modified file
    """
    try:
        # List objects in the S3 bucket with the specified prefix (directory)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Check if there are any files in the directory
        if 'Contents' not in response or len(response['Contents']) == 0:
            raise Exception("No files found in the specified directory.")

        # Sort the files by 'LastModified' in descending order (most recent first)
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)

        # Get the most recently modified file's key
        latest_file_key = files[0]['Key']
        return latest_file_key

    except Exception as e:
        print(f"Error retrieving the latest file: {str(e)}")
        return None

def lambda_handler(event, context):
    # Source S3 bucket and key for the input HTML
    input_bucket_name = os.getenv("INPUT_BUCKET")  # Replace with your input bucket name
    input_html_prefix = os.getenv("INPUT_HTML_PREFIX")  # Directory or folder in the S3 bucket
    timestamp = time.strftime('%d_%b_%Y_%H_%M_%S')
    # Destination S3 bucket and key for the generated PDF
    output_bucket_name = os.getenv("OUTPUT_BUCKET")  # Replace with your output bucket name
    output_pdf_key = f'output-pdf/reported_adverse_reaction_{timestamp}.pdf'  # Path in the bucket where the PDF will be stored

    # Path to the wkhtmltopdf binary
    wkhtmltopdf_path = os.getenv("WKHTMLTOPDF_PATH")  # Adjust this path as needed (use Lambda Layer for wkhtmltopdf)

    try:
        # Get the key of the latest HTML file in the specified directory
        input_html_key = get_latest_file_from_s3(input_bucket_name, input_html_prefix)

        # If no file is found, return an error
        if not input_html_key:
            return {
                'statusCode': 500,
                'body': json.dumps("No files found in the specified S3 folder.")
            }
        # Fetch the HTML file from the S3 bucket
        response = s3_client.get_object(Bucket=input_bucket_name, Key=input_html_key)
        html_content = response['Body'].read().decode('utf-8')  # Decode the content to string

        # Split the HTML content wherever a new <html> tag appears
        html_parts = html_content.split('<html>')

        # Ensure each part is reconstructed properly
        formatted_html_parts = [f"<html>{part.strip()}" for part in html_parts if part.strip()]

        # Add page breaks between parts
        html_with_page_breaks = "<html>".join(
            [f'{part}<div style="page-break-after: always;"></div>' for part in formatted_html_parts]
        )
        # Specify the wkhtmltopdf executable in pdfkit configuration
        config = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)

        options = {
            'orientation': 'Landscape',
            'page-size': 'A4'
            
        }

        # Function to generate PDF from a string (HTML)
        def generate_pdf_from_html(html_string):
            return pdfkit.from_string(html_string, False, configuration=config, options=options)

        # Use ThreadPoolExecutor to handle HTML parts concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            pdf_parts = list(executor.map(generate_pdf_from_html, formatted_html_parts))

        # Initialize PyPDF2 PdfMerger
        pdf_merger = PyPDF2.PdfMerger()

        # Merge each PDF part into the final PDF
        for pdf_part in pdf_parts:
            pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_part))
            pdf_merger.append(pdf_reader)

        # Write the combined PDF to a BytesIO object
        combined_pdf = BytesIO()
        pdf_merger.write(combined_pdf)
        combined_pdf.seek(0)  # Reset the stream position

        # Upload the merged PDF to the S3 bucket
        s3_client.put_object(
            Bucket=output_bucket_name,
            Key=output_pdf_key,
            Body=combined_pdf,
            ContentType='application/pdf'
        )

        return {
            'statusCode': 200,
            'body': json.dumps(f"PDF generated and uploaded to S3 at {output_pdf_key}")
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error generating PDF: {str(e)}")
        }

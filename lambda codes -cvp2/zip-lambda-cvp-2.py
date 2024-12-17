import boto3
import os
import requests
import zipfile

# Initialize S3 client
s3_client = boto3.client('s3')

# Environment Variables (Set these in Lambda configuration)
# bucket_name = os.getenv('Bucket_name')
# report_folder = os.getenv("report_folder")
# zip_url = os.getenv("zip_url")
bucket_name = "cvp-2-bucket"  # for local testing
report_folder = "Input_data/report_id_database/"  # for local testing
zip_url = "https://www.canada.ca/content/dam/hc-sc/migration/hc-sc/dhp-mps/alt_formats/zip/medeff/databasdon/extract_extrait.zip"  # for local testing

# List of allowed files
allowed_files = [
    "reports.txt",
    "report_links.txt",
    "report_drug.txt",
    "report_drug_indication.txt",
    "reactions.txt"
]

# Function to download the ZIP file
def download_zip_file():
    os.makedirs("./tmp", exist_ok=True)  # for local testing
    zip_name = os.path.basename(zip_url)
    print(f"Downloading {zip_name}...")
    response = requests.get(zip_url, stream=True)
    response.raise_for_status()

    zip_path = f"./tmp/{zip_name}"  # for local testing
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    print(f"File downloaded successfully: {zip_name}")
    return zip_path

# Function to check the contents of the ZIP file
def check_zip_contents(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_contents = zip_ref.namelist()
        print(f"Contents of the ZIP file: {zip_contents}")
        return zip_contents

# Function to check contents of the extracted files in ./tmp directory
def check_tmp_contents():
    tmp_files = os.listdir("./tmp")
    print(f"Files extracted to ./tmp: {tmp_files}")
    return tmp_files

# Function to check for new data and extract files
def check_for_new_data():
    # Download the ZIP file to /tmp directory
    zip_path = download_zip_file()

    print("Checking ZIP file contents...")
    zip_contents = check_zip_contents(zip_path)

    print("Extracting the ZIP file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall("./tmp")  # for local testing

    # Check the extracted files in ./tmp
    check_tmp_contents()

    # Process and copy allowed files to S3
    copy_allowed_files()

    # Cleanup unwanted files in the S3 bucket
    cleanup_s3_bucket()

# Function to copy allowed files to S3
def copy_allowed_files():
    subfolder = "cvponline_extract_20240831"  # Subfolder inside tmp where files are located
    for file_name in allowed_files:
        file_path = f"./tmp/{subfolder}/{file_name}"  # Updated to look inside subfolder

        if os.path.exists(file_path):
            try:
                # Upload file to S3 bucket in the report folder
                s3_client.upload_file(
                    Filename=file_path,
                    Bucket=bucket_name,
                    Key=f"{report_folder}{file_name}"
                )
                print(f"Copied {file_name} to {report_folder}{file_name} in S3")
            except Exception as e:
                print(f"Error uploading {file_name} to S3: {e}")
        else:
            print(f"{file_name} not found in ./tmp/{subfolder}. Skipping.")

# Function to cleanup unwanted files in the S3 bucket
def cleanup_s3_bucket():
    try:
        # List objects in the S3 bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=report_folder)

        # Ensure that the folder exists in the bucket, and process its contents
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                file_name = os.path.basename(file_key)

                # Only delete .txt files not in the allowed list
                if file_name.endswith(".txt") and file_name not in allowed_files:
                    print(f"Deleting {file_key} from S3...")
                    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        print("Unwanted files deleted from S3 bucket.")
    except Exception as e:
        print(f"Error cleaning up S3 bucket: {e}")

# Lambda handler function
def lambda_handler(event, context):
    check_for_new_data()
    return {
        'statusCode': 200,
        'body': 'Process completed successfully.'
    }

# for local testing
if __name__ == "__main__":
    check_for_new_data()

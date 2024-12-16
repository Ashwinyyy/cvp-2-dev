import boto3
import json
import logging
from collections import defaultdict
import time

# Initialize logging and S3 client
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
s3_client = boto3.client('s3')

# Input and output S3 buckets
input_bucket = 'cvp-2-bucket'
output_bucket = 'cvp-2-output'

# File paths in S3
drug_names_file = 'Input_data/Suspected_Product_Brand_Name/drug_names.txt'
report_drug_file = 'Input_data/report_id_database/report_drug.txt'
reports_file = 'Input_data/report_id_database/reports.txt'
reactions_file = 'Input_data/report_id_database/reactions.txt'
report_links_file = 'Input_data/report_id_database/report_links.txt'
report_drug_indication_file = 'Input_data/report_id_database/report_drug_indication.txt'


# Function to read files from S3
def read_s3_file(bucket, key):
    try:
        logging.info(f"Reading S3 file {key} from bucket {bucket}...")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        logging.error(f"Error reading S3 file {key}: {e}")
        return []


# Step 1: Parse drug names from file
def parse_drug_names(file_content):
    logging.info("Parsing drug names...")
    drug_names = []
    for line in file_content:
        line = line.strip().lower()  # Case-insensitive parsing
        if line:
            drug_names.append(line)
    logging.info(f"Parsed {len(drug_names)} drug names.")
    return drug_names


# Step 2: Locate REPORT_IDs corresponding to drug names
def find_report_ids(drug_names, report_drug_content):
    logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
    report_ids = defaultdict(list)
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = fields[3].strip().lower()
            report_id = fields[1].strip()
            if any(drug_name in line.lower() for drug_name in drug_names):
                report_ids[report_id].append(fields)
    logging.info(f"Found {len(report_ids)} report IDs matching the drug names.")
    return report_ids


# Step 3: Extract data from reference files based on REPORT_ID
def extract_report_data(report_ids):
    logging.info("Extracting report data from reference files...")
    report_data = {}

    # Read reports.txt
    reports_content = read_s3_file(input_bucket, reports_file)
    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = fields[0].strip()
            if report_id in report_ids:
                report_data[report_id] = {
                    'report_no': fields[1],
                    'version_no': fields[2],
                    'datintreceived': fields[4],
                    'datreceived': fields[3],
                    'source_eng': fields[37],
                    'mah_no': fields[5],
                    'report_type_eng': fields[7],
                    'reporter_type_eng': fields[34],
                    'seriousness_eng': fields[26],
                    'death': fields[28],
                    'disability': fields[29],
                    'congenital_anomaly': fields[30],
                    'life_threatening': fields[31],
                    'hospitalization': fields[32],
                    'other_medically_imp_cond': fields[33],
                    'age': fields[12],
                    'gender_eng': fields[10],
                    'height': fields[22],
                    'weight': fields[19],
                    'outcome_eng': fields[17]
                }

    # Read reactions.txt
    reactions_content = read_s3_file(input_bucket, reactions_file)
    for line in reactions_content:
        fields = line.split('$')
        report_id = fields[1].strip()
        if report_id in report_ids:
            report_data[report_id]['reaction_eng'] = fields[3]
            report_data[report_id]['version'] = fields[10]
            report_data[report_id]['duration'] = fields[9]

    # Read report_links.txt and add data to report_data
    report_links_content = read_s3_file(input_bucket, report_links_file)
    for line in report_links_content:
        fields = line.split('$')
        report_id = fields[1].strip()
        if report_id in report_ids:
            report_data[report_id]['link_type_eng'] = fields[1]
            report_data[report_id]['e2b_report_no'] = fields[2]

    # Read report_drug_indication.txt and add data to report_data
    report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)
    for line in report_drug_indication_content:
        fields = line.split('$')
        report_id = fields[1].strip()
        if report_id in report_ids:
            report_data[report_id]['indication_eng'] = fields[8]

    # Read report_drug.txt and add data to report_data
    report_drug_content = read_s3_file(input_bucket, report_drug_file)
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = fields[1].strip()
            if report_id in report_ids:
                # Add the new data fields from report_drug.txt
                report_data[report_id]['drug_name_eng'] = fields[3]  # DRUG_NAME_ENG
                report_data[report_id]['drug_type_eng'] = fields[5]  # DRUG_TYPE_ENG
                report_data[report_id]['dose_unit_eng'] = fields[8]  # DOSE_UNIT_ENG
                report_data[report_id]['route_eng'] = fields[16]  # ROUTE_ENG
                report_data[report_id]['dose'] = fields[7]  # DOSE
                report_data[report_id]['freq'] = fields[10]  # FREQ
                report_data[report_id]['therapy_duration'] = fields[13]  # DURATION

    logging.info(f"Extracted data for {len(report_data)} report IDs.")
    return report_data


# Step 4: Generate the JSON structure and save it to the output S3 bucket
def generate_json_output(report_data):
    logging.info("Generating JSON output...")
    final_data = []
    for report_id, data in report_data.items():
        final_data.append({
            "report_id": report_id,
            "report_no": data.get('report_no', ''),
            "version_no": data.get('version_no', ''),
            "datintreceived": data.get('datintreceived', ''),
            "datreceived": data.get('datreceived', ''),
            "source_eng": data.get('source_eng', ''),
            "mah_no": data.get('mah_no', ''),
            "report_type_eng": data.get('report_type_eng', ''),
            "reporter_type_eng": data.get('reporter_type_eng', ''),
            "seriousness_eng": data.get('seriousness_eng', ''),
            "death": data.get('death', ''),
            "disability": data.get('disability', ''),
            "congenital_anomaly": data.get('congenital_anomaly', ''),
            "life_threatening": data.get('life_threatening', ''),
            "hospitalization": data.get('hospitalization', ''),
            "other_medically_imp_cond": data.get('other_medically_imp_cond', ''),
            "age": data.get('age', ''),
            "gender_eng": data.get('gender_eng', ''),
            "height": data.get('height', ''),
            "weight": data.get('weight', ''),
            "outcome_eng": data.get('outcome_eng', ''),
            "reaction_eng": data.get('reaction_eng', ''),
            "version": data.get('version', ''),
            "duration": data.get('duration', ''),
            "link_type_eng": data.get('link_type_eng', ''),
            "e2b_report_no": data.get('e2b_report_no', ''),
            "drug_name_eng": data.get('drug_name_eng', ''),
            "drug_type_eng": data.get('drug_type_eng', ''),
            "dose_unit_eng": data.get('dose_unit_eng', ''),
            "route_eng": data.get('route_eng', ''),
            "dose": data.get('dose', ''),
            "freq": data.get('freq', ''),
            "therapy_duration": data.get('therapy_duration', ''),
            "indication_eng": data.get('indication_eng', '')
        })

    try:
        json_data = json.dumps(final_data, indent=4)
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        output_file_key = f'output_{timestamp}.json'
        logging.info(f"Uploading JSON data to S3 bucket {output_bucket} with key {output_file_key}...")
        s3_client.put_object(Bucket=output_bucket, Key=output_file_key, Body=json_data)
        logging.info(f"Successfully uploaded JSON to S3 with key {output_file_key}.")
    except Exception as e:
        logging.error(f"Error generating or uploading JSON: {e}")


# Main function to coordinate all steps
def main():
    logging.info("Starting the process...")

    # Step 1: Parse drug names
    drug_names_content = read_s3_file(input_bucket, drug_names_file)
    drug_names = parse_drug_names(drug_names_content)

    # Step 2: Find matching report IDs
    report_drug_content = read_s3_file(input_bucket, report_drug_file)
    report_ids = find_report_ids(drug_names, report_drug_content)

    # Step 3: Extract report data
    report_data = extract_report_data(report_ids)

    # Step 4: Generate and upload JSON output
    generate_json_output(report_data)

    logging.info("Process completed.")


# Run the main function
if __name__ == "__main__":
    main()

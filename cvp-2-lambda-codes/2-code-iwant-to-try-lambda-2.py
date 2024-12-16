import boto3
import json
import logging
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor

# Initialize logging and S3 client
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
s3_client = boto3.client('s3')

# Input and output S3 buckets
input_bucket = 'cvp-2-bucket'
output_bucket = 'cvp-2-output-2-testing'

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
        logging.info(f"Attempting to read S3 file {key} from bucket {bucket}...")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        logging.info(f"Successfully read S3 file {key} from bucket {bucket}.")
        return response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        logging.error(f"Error reading S3 file {key} from bucket {bucket}: {e}")
        return []


# Cleaning data
def clean_string(value):
    """Removes unwanted escape sequences and extra quotes from a string."""
    return value.strip('"').replace('\\"', '')

# Function to parse drug names
def parse_drug_names(file_content):
    logging.info("Parsing drug names...")
    return [line.strip().lower() for line in file_content if line.strip()]

# Function to parse report_drug.txt
def parse_report_drug(file_content, drug_names):
    logging.info("Parsing report_drug.txt for matching drug names...")
    report_ids = defaultdict(list)
    for line in file_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = clean_string(fields[3]).strip().lower()
            report_id = clean_string(fields[1]).strip()
            if any(drug_name in line.lower() for drug_name in drug_names):
                report_ids[report_id].append(fields)
    logging.info(f"Found {len(report_ids)} report IDs matching drug names.")
    return report_ids

# Function to parse reports.txt
def parse_reports(file_content, report_ids):
    logging.info("Parsing reports.txt...")
    report_data = {}
    for line in file_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id not in report_ids:
                continue
            report_data[report_id] = {
                'report_no': clean_string(fields[1]),
                'version_no': clean_string(fields[2]),
                'datintreceived': clean_string(fields[4]),
                'datreceived': clean_string(fields[3]),
                'source_eng': clean_string(fields[37]),
                'mah_no': clean_string(fields[5]),
                'report_type_eng': clean_string(fields[7]),
                'reporter_type_eng': clean_string(fields[34]),
                'seriousness_eng': clean_string(fields[26]),
                'death': clean_string(fields[28]),
                'disability': clean_string(fields[29]),
                'congenital_anomaly': clean_string(fields[30]),
                'life_threatening': clean_string(fields[31]),
                'hospitalization': clean_string(fields[32]),
                'other_medically_imp_cond': clean_string(fields[33]),
                'age': clean_string(fields[12]),
                'gender_eng': clean_string(fields[10]),
                'height': clean_string(fields[22]),
                'weight': clean_string(fields[19]),
                'outcome_eng': clean_string(fields[17])
            }
    return report_data

# Function to parse reactions.txt
def parse_reactions(file_content, report_ids, report_data):
    logging.info("Parsing reactions.txt...")
    for line in file_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id in report_ids:
            report_data[report_id]['reaction_eng'] = clean_string(fields[5])
            report_data[report_id]['version'] = clean_string(fields[9])
            report_data[report_id]['duration'] = clean_string(fields[2])
    return report_data

# Function to parse report_links.txt
def parse_report_links(file_content, report_ids, report_data):
    logging.info("Parsing report_links.txt...")
    for line in file_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id in report_ids:
            report_data[report_id]['link_type_eng'] = clean_string(fields[2])
            report_data[report_id]['e2b_report_no'] = clean_string(fields[4])
    return report_data

# Function to parse report_drug_indication.txt
def parse_report_drug_indication(file_content, report_ids, report_data):
    logging.info("Parsing report_drug_indication.txt...")
    indication_report_ids = set()
    for line in file_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id in report_ids:
            indication_report_ids.add(report_id)

    for report_id in report_ids:
        if report_id in indication_report_ids:
            for line in file_content:
                fields = line.split('$')
                report_id_in_line = clean_string(fields[1]).strip()
                if report_id_in_line == report_id:
                    report_data[report_id]['indication_eng'] = clean_string(fields[4])
    return report_data

# Function to parse report_drug.txt
def parse_report_drug(file_content, report_ids, report_data):
    logging.info("Parsing report_drug.txt...")
    for line in file_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id in report_ids:
            drug_name = clean_string(fields[3])
            if 'drug_name_eng' in report_data[report_id]:
                report_data[report_id]['drug_name_eng'] += ', ' + drug_name
            else:
                report_data[report_id]['drug_name_eng'] = drug_name
            report_data[report_id]['drug_type_eng'] = clean_string(fields[4])
            report_data[report_id]['dose_unit_eng'] = clean_string(fields[9])
            report_data[report_id]['route_eng'] = clean_string(fields[6])
            report_data[report_id]['dose'] = clean_string(fields[8])
            report_data[report_id]['freq'] = clean_string(fields[11])
            report_data[report_id]['therapy_duration'] = clean_string(fields[17])
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
            "indication_eng": data.get('indication_eng', ''),
        })

    # Save the data to a new JSON file
    output_file_key = 'final_data.json'
    s3_client.put_object(Bucket=output_bucket, Key=output_file_key, Body=json.dumps(final_data))
    logging.info(f"JSON output saved to S3 bucket: {output_bucket}/{output_file_key}")


def main():
    # Step 1: Read and process files concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        drug_names_future = executor.submit(read_s3_file, input_bucket, drug_names_file)
        report_drug_future = executor.submit(read_s3_file, input_bucket, report_drug_file)
        reports_future = executor.submit(read_s3_file, input_bucket, reports_file)
        reactions_future = executor.submit(read_s3_file, input_bucket, reactions_file)
        report_links_future = executor.submit(read_s3_file, input_bucket, report_links_file)
        report_drug_indication_future = executor.submit(read_s3_file, input_bucket, report_drug_indication_file)

        # Wait for file processing to complete
        drug_names_content = drug_names_future.result()
        report_drug_content = report_drug_future.result()
        reports_content = reports_future.result()
        reactions_content = reactions_future.result()
        report_links_content = report_links_future.result()
        report_drug_indication_content = report_drug_indication_future.result()

    # Step 2: Parse files
    drug_names = parse_drug_names(drug_names_content)
    report_ids = parse_report_drug(report_drug_content, drug_names)
    report_data = parse_reports(reports_content, report_ids)
    report_data = parse_reactions(reactions_content, report_ids, report_data)
    report_data = parse_report_links(report_links_content, report_ids, report_data)
    report_data = parse_report_drug_indication(report_drug_indication_content, report_ids, report_data)

    # Step 3: Generate JSON and upload to S3
    generate_json_output(report_data)


if __name__ == '__main__':
    main()

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
    """Removes unwanted escape sequences and extra quotes from a JSON string."""
    return value.strip('"').replace('\\"', '')

# Step 1: Parse drug names from file
def parse_drug_names(file_content):
    logging.info("Parsing drug names...")
    drug_names = [line.strip().lower() for line in file_content if line.strip()]
    logging.info(f"Parsed {len(drug_names)} drug names.")
    return drug_names


# Step 2: Locate REPORT_IDs corresponding to drug names
def find_report_ids(drug_names, report_drug_content):
    logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
    report_ids = defaultdict(list)
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = clean_string(fields[3]).strip().lower()
            report_id = clean_string(fields[1]).strip()
            if any(drug_name in line.lower() for drug_name in drug_names):
                report_ids[report_id].append(fields)
    logging.info(f"Found {len(report_ids)} report IDs matching the drug names.")
    return report_ids


# Step 3: Extract data from reference files based on REPORT_ID
def extract_report_data(report_ids, reports_content, reactions_content, report_links_content,
                        report_drug_indication_content, report_drug_content):
    logging.info("Extracting report data from reference files...")
    report_data = {}

    # Read reports.txt and build report_data
    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
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

    # Read reactions.txt
    for line in reactions_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        report_data[report_id]['reaction_eng'] = clean_string(fields[5])
        report_data[report_id]['version'] = clean_string(fields[9])
        report_data[report_id]['duration'] = clean_string(fields[2])

    # Read report_links.txt
    for line in report_links_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        report_data[report_id]['link_type_eng'] = clean_string(fields[2])
        report_data[report_id]['e2b_report_no'] = clean_string(fields[4])

    # Read report_drug_indication.txt
    indication_report_ids = set()
    for line in report_drug_indication_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
            indication_report_ids.add(report_id)

    # Compare report_ids with indication_report_ids
    for report_id in report_ids:
        if report_id in indication_report_ids:
            for line in report_drug_indication_content:
                fields = line.split('$')
                report_id_in_line = clean_string(fields[1]).strip()
                if report_id_in_line == report_id:
                    report_data[report_id]['indication_eng'] = clean_string(fields[4])

    # Read report_drug.txt and accumulate drug names for each report_id
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
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
            "indication_eng": data.get('indication_eng', ''),
        })

    output_key = 'Output_data/processed_report_data.json'
    try:
        logging.info(f"Saving JSON output to {output_key} in S3 bucket {output_bucket}...")
        s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=json.dumps(final_data))
        logging.info(f"Successfully saved JSON output to {output_key} in S3.")
    except Exception as e:
        logging.error(f"Error saving JSON output to S3: {e}")


# Step 5: Process the data in parallel using ThreadPoolExecutor
def process_data_concurrently(max_workers=4):
    logging.info("Starting concurrent data processing...")

    # Reading the files
    drug_names_content = read_s3_file(input_bucket, drug_names_file)
    report_drug_content = read_s3_file(input_bucket, report_drug_file)
    reports_content = read_s3_file(input_bucket, reports_file)
    reactions_content = read_s3_file(input_bucket, reactions_file)
    report_links_content = read_s3_file(input_bucket, report_links_file)
    report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)

    # Parsing the data
    drug_names = parse_drug_names(drug_names_content)

    # Find the report_ids for the given drug names
    report_ids = find_report_ids(drug_names, report_drug_content)

    # Extract data for those report_ids
    report_data = extract_report_data(report_ids, reports_content, reactions_content, report_links_content,
                                      report_drug_indication_content, report_drug_content)

    # Save the processed data to S3
    generate_json_output(report_data)


# Running the concurrent processing
if __name__ == "__main__":
    start_time = time.time()

    # Adjust the max_workers based on the desired number of threads
    max_workers = 4  # You can change this number as needed
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.submit(process_data_concurrently, max_workers)

    logging.info(f"Processing completed in {time.time() - start_time:.2f} seconds.")



# # Step 5: Process the data in parallel using ThreadPoolExecutor
# def process_data_concurrently():
#     logging.info("Starting concurrent data processing...")

#     # Reading the files
#     drug_names_content = read_s3_file(input_bucket, drug_names_file)
#     report_drug_content = read_s3_file(input_bucket, report_drug_file)
#     reports_content = read_s3_file(input_bucket, reports_file)
#     reactions_content = read_s3_file(input_bucket, reactions_file)
#     report_links_content = read_s3_file(input_bucket, report_links_file)
#     report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)

#     # Parsing the data
#     drug_names = parse_drug_names(drug_names_content)

#     # Find the report_ids for the given drug names
#     report_ids = find_report_ids(drug_names, report_drug_content)

#     # Extract data for those report_ids
#     report_data = extract_report_data(report_ids, reports_content, reactions_content, report_links_content,
#                                       report_drug_indication_content, report_drug_content)

#     # Save the processed data to S3
#     generate_json_output(report_data)


# # Running the concurrent processing
# if __name__ == "__main__":
#     start_time = time.time()

#     with ThreadPoolExecutor() as executor:
#         executor.submit(process_data_concurrently)

#     logging.info(f"Processing completed in {time.time() - start_time:.2f} seconds.")

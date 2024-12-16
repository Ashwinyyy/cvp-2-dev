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


#cleaning data
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
            drug_name = fields[3].strip().lower()
            report_id = fields[1].strip()
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
            report_id = fields[0].strip()
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
        report_id = fields[1].strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        report_data[report_id]['reaction_eng'] = clean_string(fields[5])
        report_data[report_id]['version'] = clean_string(fields[9])
        report_data[report_id]['duration'] = clean_string(fields[2])

    # Read report_links.txt
    for line in report_links_content:
        fields = line.split('$')
        report_id = fields[1].strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        report_data[report_id]['link_type_eng'] = clean_string(fields[2])
        report_data[report_id]['e2b_report_no'] = clean_string(fields[4])

    # Read report_drug_indication.txt
    # # Read report_drug_indication.txt
    # for line in report_drug_indication_content:
    #     fields = line.split('$')
    #     report_id = fields[1].strip()
    #
    #     # Check if the report_id is in the report_ids set
    #     if report_id not in report_ids:
    #         report_data[report_id] = {'indication_eng': ''}  # Set blank if no match
    #         continue  # Skip further processing if the report_id is not in the report_ids
    #
    #     # If there's a match, process the indication
    #     report_data[report_id]['indication_eng'] = fields[4] if len(fields) > 4 else ''

#reading report_drug_indication.txt
    # Read the file content
    logging.info("Reading report_drug_indication.txt...")
    report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)

    # Initialize a set to store all report_ids found in the file
    indication_report_ids = set()

    if report_drug_indication_content:  # If the file has content
        for line in report_drug_indication_content:
            # Split the line into fields using '$' as the delimiter
            fields = line.split('$')

            # Check if the line has enough fields to process
            if len(fields) > 1:
                # Extract the report_id from field[1] and strip any whitespace
                report_id = fields[1].strip()

                # Skip the line if report_id is not in the report_ids list
                if report_id not in report_ids:
                    continue  # Skip if the report_id is not in the report_ids

                # Add the report_id to the set of indication_report_ids
                indication_report_ids.add(report_id)
    else:
        logging.info("No content found in report_drug_indication.txt.")

    # Compare report_ids with indication_report_ids
    logging.info("Comparing report_ids with report IDs from indication file...")
    for report_id in report_ids:
        if report_id in indication_report_ids:  # Proceed only if report_id exists in the indication file
            # Get the corresponding line and extract the indication_eng field
            for line in report_drug_indication_content:
                fields = line.split('$')

                # Check if the line is valid (contains more than one field)
                if len(fields) > 1:
                    # Extract the report_id from field[1] and check if it matches
                    report_id_in_line = fields[1].strip()

                    if report_id_in_line == report_id:
                        # Add the clean indication_eng field to report_data
                        report_data[report_id]['indication_eng'] = clean_string(fields[4])
        else:
            logging.info(f"Skipping report_id {report_id} as it is not in the indication file.")

    # Read report_drug.txt
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = fields[1].strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
            # Add the new data fields from report_drug.txt
            report_data[report_id]['drug_name_eng'] = clean_string(fields[3] ) # DRUG_NAME_ENG
            report_data[report_id]['drug_type_eng'] = clean_string(fields[4]  )# DRUG_TYPE_ENG
            report_data[report_id]['dose_unit_eng'] = clean_string(fields[9])  # DOSE_UNIT_ENG
            report_data[report_id]['route_eng'] = clean_string(fields[6] ) # ROUTE_ENG
            report_data[report_id]['dose'] = clean_string(fields[8] ) # DOSE
            report_data[report_id]['freq'] = clean_string(fields[11])  # FREQ
            report_data[report_id]['therapy_duration'] = clean_string(fields[17] ) # DURATION

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
            "indication_eng": data.get('indication_eng', '')  # Indication left blank if not found
        })

    try:
        json_data = json.dumps(final_data, indent=4)
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        output_file = f"output_data_{timestamp}.json"
        s3_client.put_object(Bucket=output_bucket, Key=output_file, Body=json_data)
        logging.info(f"Successfully uploaded JSON file to S3: {output_file}")
    except Exception as e:
        logging.error(f"Error generating or uploading JSON output: {e}")


# Main function to execute all steps in parallel
def main():
    logging.info("Starting script execution...")
    start_time = time.time()

    # Read input files in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # Submit S3 read tasks
        futures = {
            'drug_names': executor.submit(read_s3_file, input_bucket, drug_names_file),
            'report_drug': executor.submit(read_s3_file, input_bucket, report_drug_file),
            'reports': executor.submit(read_s3_file, input_bucket, reports_file),
            'reactions': executor.submit(read_s3_file, input_bucket, reactions_file),
            'report_links': executor.submit(read_s3_file, input_bucket, report_links_file),
            'report_drug_indication': executor.submit(read_s3_file, input_bucket, report_drug_indication_file)
        }

        # Wait for all read tasks to finish
        data = {key: future.result() for key, future in futures.items()}

    # Step 1: Parse drug names
    drug_names = parse_drug_names(data['drug_names'])

    # Step 2: Find report IDs corresponding to drug names
    report_ids = find_report_ids(drug_names, data['report_drug'])

    # Step 3: Extract data based on report IDs
    report_data = extract_report_data(report_ids, data['reports'], data['reactions'], data['report_links'],
                                      data['report_drug_indication'], data['report_drug'])

    # Step 4: Generate and save the JSON output to S3
    generate_json_output(report_data)

    logging.info(f"Script execution completed in {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()

import boto3
import json
import logging
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import io


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
        logging.info(f"Attempting to read S3 file {key} from bucket {bucket}...")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        logging.info(f"Successfully read S3 file {key} from bucket {bucket}.")
        return response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        logging.error(f"Error reading S3 file {key} from bucket {bucket}: {e}")
        return []


# converting date format
def convert_date_format(date_str):
    try:
        # Convert the date string from 'DD-MMM-YY' to 'YYYY-MM-DD'
        date_obj = datetime.strptime(date_str, "%d-%b-%y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        # Return the original string if it doesn't match the expected format
        return date_str


# coverting 1 to yes, 2 to no
def convert_to_yes_no(value):
    if value == "1":
        return "yes"
    elif value == "2":
        return "no"
    else:
        return value  # In case there are other values, return the original value


# cleaning data
def clean_string(value):
    """Removes unwanted escape sequences and extra quotes from a JSON string."""
    return value.strip('"').replace('\\"', '')


# Step 1: Parse drug names from file
def parse_drug_names(file_content):
    logging.info("Parsing drug names...")
    drug_names = set()  # Use a set to store unique drug names
    for line in file_content:
        drug_name = line.strip().lower()  # Normalize drug name to lowercase
        if drug_name:  # Ignore blank lines
            drug_names.add(drug_name)  # Add to set (duplicates automatically removed)
    logging.info(f"Parsed {len(drug_names)} unique drug names.")
    return list(drug_names)  # Convert back to list if needed



# Step 2: Locate REPORT_IDs corresponding to drug names
def find_report_ids(drug_names, report_drug_content):
    logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
    report_ids = defaultdict(list)

    # Convert the list of drug names to a set for faster lookup
    drug_names_set = set(drug_names)

    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = clean_string(fields[3]).strip().lower()  # Normalize drug name to lowercase
            report_id = clean_string(fields[1]).strip()

            # Check if any drug name is a substring in the field (fields[3])
            for name in drug_names_set:
                if name.lower() in drug_name:  # Check if the drug name is a substring of fields[3]
                    report_ids[report_id].append(fields)
                    break  # Stop checking other drug names if a match is found

    logging.info(f"Found {len(report_ids)} report IDs matching the drug names.")
    return report_ids

def filter_report_ids_by_source(report_ids, reports_content):
    logging.info(f"Filtering REPORT_IDs based on SOURCE_ENG...")

    # Only consider the REPORT_IDs in the 374 found earlier
    report_ids_set = set(report_ids.keys())  # Convert 374 report IDs to a set
    report_ids_to_remove = set()

    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 37:  # Ensure fields[37] (SOURCE_ENG) exists
            report_id = clean_string(fields[0]).strip()
            source_eng = clean_string(fields[37]).strip().lower()

            # Check SOURCE_ENG for "mah" only for relevant REPORT_IDs
            if report_id in report_ids_set and "mah" in source_eng:
                report_ids_to_remove.add(report_id)

    # Filter out REPORT_IDs to remove
    filtered_report_ids = {rid: details for rid, details in report_ids.items() if rid not in report_ids_to_remove}

    logging.info(f"Initial REPORT_IDs: {len(report_ids)}")
    logging.info(f"Excluded REPORT_IDs: {len(report_ids_to_remove)}")
    logging.info(f"Remaining REPORT_IDs: {len(filtered_report_ids)}")

    return filtered_report_ids


def extract_report_data(report_ids, reports_content, reactions_content, report_drug_indication_content, report_links_content, report_drug_content):
    logging.info("Extracting report data from reference files...")
    report_data = {}

    # Step 1: Process reports.txt first
    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
            report_data[report_id] = {
                'report_no': clean_string(fields[1]),
                'version_no': clean_string(fields[2]),
                'datreceived': convert_date_format(clean_string(fields[3])),
                'datintreceived': convert_date_format(clean_string(fields[4])),
                'mah_no': clean_string(fields[5]),
                'report_type_eng': clean_string(fields[7]),
                'gender_eng': clean_string(fields[10]),
                'age': clean_string(fields[12]),
                'age_unit_eng': clean_string(fields[14]),
                'outcome_eng': clean_string(fields[17]),
                'weight': clean_string(fields[19]),
                'weight_unit_eng': clean_string(fields[20]),
                'height': clean_string(fields[22]),
                'height_unit_eng': clean_string(fields[23]),
                'seriousness_eng': clean_string(fields[26]),
                'death': convert_to_yes_no(clean_string(fields[28])),
                'disability': convert_to_yes_no(clean_string(fields[29])),
                'congenital_anomaly': convert_to_yes_no(clean_string(fields[30])),
                'life_threatening': convert_to_yes_no(clean_string(fields[31])),
                'hospitalization': convert_to_yes_no(clean_string(fields[32])),
                'other_medically_imp_cond': convert_to_yes_no(clean_string(fields[33])),
                'reporter_type_eng': clean_string(fields[34]),
                'source_eng': clean_string(fields[37])
            }

    # Step 2: Process reactions.txt
    for line in reactions_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        pt_name_eng = clean_string(fields[5])
        meddra_version = clean_string(fields[9])
        duration = clean_string(fields[2])
        duration_unit_eng = clean_string(fields[3])
        if 'pt_name_eng' in report_data[report_id]:
            report_data[report_id]['pt_name_eng'] += ', ' + pt_name_eng
            report_data[report_id]['meddra_version'] += ', ' + meddra_version
            report_data[report_id]['duration'] += ', ' + duration
            report_data[report_id]['duration_unit_eng'] += ', ' + duration_unit_eng
        else:
            report_data[report_id]['pt_name_eng'] = pt_name_eng
            report_data[report_id]['meddra_version'] = meddra_version
            report_data[report_id]['duration'] = duration
            report_data[report_id]['duration_unit_eng'] = duration_unit_eng

    # Step 3: Process report_links.txt
    matched_ids = set()  # To track report_ids found in report_links.txt

    for line in report_links_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        record_type_eng = clean_string(fields[2]).strip()
        report_link_no = clean_string(fields[4]).strip()

        # Process only if the report_id is in report_ids
        if report_id in report_ids:
            # Initialize the report_data entry if it's not already present
            if report_id not in report_data:
                report_data[report_id] = {}

            # Assign values from the line
            report_data[report_id]['record_type_eng'] = record_type_eng
            report_data[report_id]['report_link_no'] = report_link_no

            # Mark this report_id as matched
            matched_ids.add(report_id)

    # Handle report_ids that were not matched
    for report_id in report_ids:
        if report_id not in matched_ids:
            # Ensure only missing fields are updated without overwriting existing data
            if report_id not in report_data:
                report_data[report_id] = {}  # Initialize if not present
            report_data[report_id].setdefault('record_type_eng', 'No duplicate or linked report')
            report_data[report_id].setdefault('report_link_no', 'No duplicate or linked report')

    # Step 4: Process report_drug.txt
    drug_names_dict = {}
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
            drug_name = clean_string(fields[3])
            drug_involvement = clean_string(fields[4])
            route_admin = clean_string(fields[6])
            unit_dose_qty = clean_string(fields[8])
            dose_unit_eng = clean_string(fields[9])
            freq_time_unit_eng = clean_string(fields[15])
            therapy_duration = clean_string(fields[17])
            therapy_duration_unit_eng = clean_string(fields[18])
            dosageform_eng = clean_string(fields[20])

            # Initialize drug_names_dict and report_data
            if report_id not in drug_names_dict:
                drug_names_dict[report_id] = []
            drug_names_dict[report_id].append(drug_name)  # Add drug name to the list for this report_id

            if report_id not in report_data:
                report_data[report_id] = {}
            if 'drug_name' in report_data[report_id]:
                report_data[report_id]['drug_name'] += ', ' + drug_name
            else:
                report_data[report_id]['drug_name'] = drug_name

           # Append or initialize for 'drug_involvement'
            if 'drug_involvement' in report_data[report_id]:
                report_data[report_id]['drug_involvement'] += ', ' + drug_involvement
            else:
                report_data[report_id]['drug_involvement'] = drug_involvement

            # Append or initialize for 'route_admin'
            if 'route_admin' in report_data[report_id]:
                report_data[report_id]['route_admin'] += ', ' + route_admin
            else:
                report_data[report_id]['route_admin'] = route_admin

            # Append or initialize for 'unit_dose_qty'
            if 'unit_dose_qty' in report_data[report_id]:
                report_data[report_id]['unit_dose_qty'] += ', ' + unit_dose_qty
            else:
                report_data[report_id]['unit_dose_qty'] = unit_dose_qty

            # Append or initialize for 'dose_unit_eng'
            if 'dose_unit_eng' in report_data[report_id]:
                report_data[report_id]['dose_unit_eng'] += ', ' + dose_unit_eng
            else:
                report_data[report_id]['dose_unit_eng'] = dose_unit_eng

            # Append or initialize for 'freq_time_unit_eng'
            if 'freq_time_unit_eng' in report_data[report_id]:
                report_data[report_id]['freq_time_unit_eng'] += ', ' + freq_time_unit_eng
            else:
                report_data[report_id]['freq_time_unit_eng'] = freq_time_unit_eng

            # Append or initialize for 'therapy_duration'
            if 'therapy_duration' in report_data[report_id]:
                report_data[report_id]['therapy_duration'] += ', ' + therapy_duration
            else:
                report_data[report_id]['therapy_duration'] = therapy_duration

            # Append or initialize for 'therapy_duration_unit_eng'
            if 'therapy_duration_unit_eng' in report_data[report_id]:
                report_data[report_id]['therapy_duration_unit_eng'] += ', ' + therapy_duration_unit_eng
            else:
                report_data[report_id]['therapy_duration_unit_eng'] = therapy_duration_unit_eng

            # Append or initialize for 'therapy_duration_unit_eng'
            if 'dosageform_eng' in report_data[report_id]:
                report_data[report_id]['dosageform_eng'] += ', ' + dosageform_eng
            else:
                report_data[report_id]['dosageform_eng'] = dosageform_eng

    # Step 5: Process report_drug_indication.txt after all other files
    for line in report_drug_indication_content:
        fields = line.split('$')
        if len(fields) > 4:
            report_id = clean_string(fields[1]).strip()
            drug_name_eng = clean_string(fields[3]).strip().lower()
            indication = clean_string(fields[4]).strip()

            if report_id not in report_ids:
                continue

            # Get the list of drug names for the current report_id
            drug_names_for_report = drug_names_dict.get(report_id, [])

            # Initialize indication_eng if it doesn't exist
            if 'indication_eng' not in report_data[report_id]:
                # Placeholder for each drug: a space separated by commas
                report_data[report_id]['indication_eng'] = ' , ' * (len(drug_names_for_report) - 1) + ' '

            # Find the drug index and assign the correct indication to that index
            for index, drug_name in enumerate(drug_names_for_report):
                # Match the drug name with its indication if it exists
                if drug_name_eng == drug_name.lower():
                    indication_list = report_data[report_id]['indication_eng'].split(', ')
                    indication_list[index] = indication.strip()  # Assign the indication to the correct drug
                    report_data[report_id]['indication_eng'] = ', '.join(indication_list)

    return report_data


# Step 4: Generate the JSON structure and save it to the output S3 bucket
def generate_json_output(report_data):
    logging.info("Generating JSON output...")
    final_data = []
    for report_id, data in report_data.items():
        final_data. append({
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
                "age_unit_eng": data.get('age_unit_eng', ''),
                "gender_eng": data.get('gender_eng', ''),
                "height": data.get('height', ''),
                "height_unit_eng": data.get('height_unit_eng', ''),
                "weight": data.get('weight', ''),
                "weight_unit_eng": data.get('weight_unit_eng', ''),
                "outcome_eng": data.get('outcome_eng', ''),
                "record_type_eng": data.get('record_type_eng', ''),
                "report_link_no": data.get('report_link_no', ''),
                "drug_name": data.get('drug_name', ''),
                "drug_involvement": data.get('drug_involvement', ''),
                "dosage_form_eng": data.get('dosageform_eng', ''),
                "route_admin": data.get('route_admin', ''),
                "unit_dose_qty": data.get('unit_dose_qty', ''),
                "dose_unit_eng": data.get('dose_unit_eng', ''),
                "freq_time_unit_eng": data.get('freq_time_unit_eng', ''),
                "therapy_duration": data.get('therapy_duration', ''),
                "therapy_duration_unit_eng": data.get('therapy_duration_unit_eng', ''),
                "indication_eng": data.get('indication_eng', ''),
                "pt_name_eng": data.get('pt_name_eng', ''),
                "meddra_version": data.get('meddra_version', ''),
                "duration": data.get('duration', ''),
                "duration_unit_eng": data.get('duration_unit_eng', '')
            })

    try:
        json_data = json.dumps(final_data, indent=4)
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        output_file = f"report_output/output_data_{timestamp}.json"
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
    logging.info("Starting parsing drugnames...")
    drug_names = parse_drug_names(data['drug_names'])

     # Step 2: Find report IDs corresponding to drug names
    filter_report_ids = find_report_ids(drug_names, data['report_drug'])

    report_ids = filter_report_ids_by_source(filter_report_ids, data['reports'])

    # Step 3: Extract data based on report IDs
    report_data = extract_report_data(report_ids, data['reports'], data['reactions'], data['report_drug_indication'], data['report_links'], data['report_drug'])

    # Step 4: Generate and save the JSON output to S3
    generate_json_output(report_data)

    logging.info(f"Script execution completed in {time.time() - start_time:.2f} seconds.")


# Lambda handler (can be used in AWS Lambda environment)
def lambda_handler(event, context):
    logging.info("Lambda function started.")

    # Simulate parallel S3 reading in AWS Lambda by calling main function (in a single thread for Lambda)
    main()

    return {
        'statusCode': 200,
        'body': json.dumps('Processing completed successfully.')
    }

if __name__ == "__main__":
    main()

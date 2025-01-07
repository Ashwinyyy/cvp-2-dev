import json
import boto3


def load_json_from_s3(bucket_name, directory):
    """Fetch the latest JSON file from the specified S3 directory."""
    s3_client = boto3.client('s3')

    # List all objects in the given directory
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
        files = response.get('Contents', [])

        if not files:
            print(f"No files found in {directory}.")
            return None

        # Sort files by last modified date, descending order
        files.sort(key=lambda x: x['LastModified'], reverse=True)

        # Get the most recent file
        latest_file = files[0]['Key']
        print(f"Latest file: {latest_file}")

        # Fetch the latest file from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file)
        file_content = file_obj['Body'].read().decode('utf-8')

        # Parse JSON content
        json_data = json.loads(file_content)
        return json_data

    except Exception as e:
        print(f"Error loading JSON from S3: {e}")
        return None


def split_comma_values(value):
    """Helper function to split comma-separated values and remove placeholders."""
    placeholders = ["{{health_product_role}}", "{{dosage_form}}", "{{route_of_administration}}",
                    "{{dose}}", "{{frequency}}", "{{therapy_duration}}", "{{indication}}",
                    "{{meddra_version}}", "{{reaction_duration}}"]
    values = [v.strip() for v in value.split(',') if v.strip() not in placeholders]
    return values


def format_combined_values(quantity, unit):
    """Combine quantity and unit into a single string."""
    return f"{quantity} {unit}" if quantity and unit else ""


def generate_html_from_template(item, formatted_data):
    """Generate HTML content for one report."""
    # Read the template HTML from local directory
    with open('template.html', 'r') as file:
        html_template = file.read()

    # Replace placeholders in the HTML template with dynamic values
    html_template = html_template.replace('{{adverse_reaction_report_number}}', item.get('report_no', ''))
    html_template = html_template.replace('{{latest_aer_version_number}}', item.get('version_no', ''))
    html_template = html_template.replace('{{initial_received_date}}', item.get('datintreceived', ''))
    html_template = html_template.replace('{{latest_received_date}}', item.get('datreceived', ''))
    html_template = html_template.replace('{{source_of_report}}', item.get('source_eng', ''))
    html_template = html_template.replace('{{market_authorization_holder_aer_number}}', item.get('mah_no', ''))
    html_template = html_template.replace('{{type_of_report}}', item.get('report_type_eng', ''))
    html_template = html_template.replace('{{reporter_type}}', item.get('reporter_type_eng', ''))

    # Replace side-table (death, disability, etc.)
    html_template = html_template.replace('{{death}}', item.get('death', ''))
    html_template = html_template.replace('{{disability}}', item.get('disability', ''))
    html_template = html_template.replace('{{anomaly}}', item.get('congenital_anomaly', ''))
    html_template = html_template.replace('{{life_threatening}}', item.get('life_threatening', ''))
    html_template = html_template.replace('{{hospitalization}}', item.get('hospitalization', ''))
    html_template = html_template.replace('{{other_conditions}}', item.get('other_medically_imp_cond', ''))

    # Patient info
    html_template = html_template.replace('{{age}}', item.get('age', '') + ' ' + item.get('age_unit_eng', ''))
    html_template = html_template.replace('{{gender}}', item.get('gender_eng', ''))
    html_template = html_template.replace('{{height}}', item.get('height', '') + ' ' + item.get('height_unit_eng', ''))
    html_template = html_template.replace('{{weight}}', item.get('weight', '') + ' ' + item.get('weight_unit_eng', ''))
    html_template = html_template.replace('{{report_outcome}}', item.get('outcome_eng', ''))
    html_template = html_template.replace('{{record_type}}', item.get('record_type_eng', ''))
    html_template = html_template.replace('{{link_aer_number}}', item.get('report_link_no', ''))

    # Format product rows to prevent nesting
    product_rows = ""
    # Find the maximum length of the product-related lists
    max_length = max(len(formatted_data[key]) for key in
                     ['drug_name', 'drug_involvement', 'dosage_form', 'route', 'dose', 'freq_time', 'therapy_duration',
                      'indication'])

    # Iterate over the maximum length, ensuring we don't exceed the index range of any list
    for i in range(max_length):
        drug_name = formatted_data['drug_name'][i] if i < len(formatted_data['drug_name']) else ""
        drug_involvement = formatted_data['drug_involvement'][i] if i < len(formatted_data['drug_involvement']) else ""
        dosage_form = formatted_data['dosage_form'][i] if i < len(formatted_data['dosage_form']) else ""
        route = formatted_data['route'][i] if i < len(formatted_data['route']) else ""
        dose = formatted_data['dose'][i] if i < len(formatted_data['dose']) else ""
        freq_time = formatted_data['freq_time'][i] if i < len(formatted_data['freq_time']) else ""
        therapy_duration = formatted_data['therapy_duration'][i] if i < len(formatted_data['therapy_duration']) else ""
        indication = formatted_data['indication'][i] if i < len(formatted_data['indication']) else ""

        # Add a row for the product information
        product_rows += f"<tr><td>{drug_name}</td><td>{drug_involvement}</td><td>{dosage_form}</td><td>{route}</td><td>{dose}</td><td>{freq_time}</td><td>{therapy_duration}</td><td>{indication}</td></tr>"

    if not product_rows.strip():
        product_rows = "<tr><td colspan='8'>No product data available</td></tr>"

    html_template = html_template.replace('{{product_description}}', product_rows)

    # Format adverse reaction rows
    adverse_reaction_rows = ""
    for i in range(len(formatted_data['pt_name'])):
        adverse_reaction_rows += f"<tr><td>{formatted_data['pt_name'][i]}</td><td>{formatted_data['meddra_version'][i]}</td><td>{formatted_data['duration'][i]} {formatted_data['duration_unit'][i]}</td></tr>"

    if not adverse_reaction_rows.strip():
        adverse_reaction_rows = "<tr><td colspan='3'>No adverse reaction data available</td></tr>"

    html_template = html_template.replace('{{adverse_reaction_terms}}', adverse_reaction_rows)

    return html_template


def format_data(item):
    """Formats the data and handles comma-separated values."""
    fields = {
        'drug_name': split_comma_values(item.get('drug_name', '')),
        'drug_involvement': split_comma_values(item.get('drug_involvement', '')),
        'dosage_form': split_comma_values(item.get('dosage_form_eng', '')),
        'route': split_comma_values(item.get('route_admin', '')),
        'unit_dose': split_comma_values(item.get('unit_dose_qty', '')),
        'dose_unit': split_comma_values(item.get('dose_unit_eng', '')),
        'freq_time': split_comma_values(item.get('freq_time_unit_eng', '')),
        'therapy_duration': split_comma_values(item.get('therapy_duration', '')),
        'therapy_unit': split_comma_values(item.get('therapy_duration_unit_eng', '')),
        'indication': split_comma_values(item.get('indication_eng', '')),
        'pt_name': split_comma_values(item.get('pt_name_eng', '')),
        'meddra_version': split_comma_values(item.get('meddra_version', '')),
        'duration': split_comma_values(item.get('duration', '')),
        'duration_unit': split_comma_values(item.get('duration_unit_eng', '')),
    }

    fields['dose'] = [format_combined_values(qty, unit) for qty, unit in zip(fields['unit_dose'], fields['dose_unit'])]
    fields['therapy_duration'] = [format_combined_values(dur, unit) for dur, unit in
                                  zip(fields['therapy_duration'], fields['therapy_unit'])]

    return fields


def generate_input_html(json_data):
    """Generate the complete input.html file that contains all reports."""
    input_html = "<html><head><title>Adverse Event Reports</title></head><body>"

    # Generate a section for each report
    report_sections = []
    for index, item in enumerate(json_data):
        formatted_data = format_data(item)
        html_content = generate_html_from_template(item, formatted_data)

        # Add the actual content of the report with an anchor link
        report_sections.append(
            f'<section id="report_{item["report_no"]}">{html_content}</section>')

    input_html += "\n".join(report_sections)
    input_html += "</body></html>"

    return input_html


def upload_html_to_s3(html_content, bucket_name, file_name):
    """Upload the generated HTML content to S3 bucket."""
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=html_content, Bucket=bucket_name, Key=file_name, ContentType='text/html')


def main():
    # S3 bucket details
    input_bucket = 'cvp-2-output'  # Bucket containing the report_output directory
    output_bucket = 'cvp-2-bucket'  # Bucket to upload the generated HTML
    directory = 'report_output/'  # Directory in the input bucket containing the JSON files
    output_html_file_key = 'input-html/input.html'  # Path in the output bucket where the file will be uploaded

    # Load the latest JSON data from S3
    json_data = load_json_from_s3(input_bucket, directory)

    if json_data:
        # Generate the input HTML
        input_html = generate_input_html(json_data)

        # Upload the HTML file to S3
        upload_html_to_s3(input_html, output_bucket, output_html_file_key)
        print(f"HTML content successfully uploaded to {output_bucket}/{output_html_file_key}")
    else:
        print("Failed to load JSON data from S3.")


if __name__ == "__main__":
    main()

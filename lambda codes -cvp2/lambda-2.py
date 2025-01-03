import json
import boto3
from datetime import datetime

# Sample data for testing (same as before)
json_data = [
    {
        "report_no": "000585940",
        "version_no": "0",
        "datintreceived": "2014-02-04",
        "datreceived": "2014-02-04",
        "source_eng": "Hospital",
        "mah_no": "",
        "report_type_eng": "Spontaneous",
        "reporter_type_eng": "Other health professional",
        "seriousness_eng": "Not Serious",
        "death": "",
        "disability": "",
        "congenital_anomaly": "",
        "life_threatening": "",
        "hospitalization": "",
        "other_medically_imp_cond": "",
        "age": "",
        "age_unit_eng": "",
        "gender_eng": "Female",
        "height": "",
        "height_unit_eng": "",
        "weight": "9.94",
        "weight_unit_eng": "Kilogram",
        "outcome_eng": "Unknown",
        "record_type_eng": "No duplicate or linked report",
        "report_link_no": "No duplicate or linked report",
        "drug_name": "TYLENOL, OCTAGAM 10% FOR I.V. INFUSION, BENADRYL",
        "drug_involvement": "Concomitant, Suspect, Concomitant",
        "dosage_form_eng": "NOT SPECIFIED, SOLUTION INTRAVENOUS, NOT SPECIFIED",
        "route_admin": ", Intravenous (not otherwise specified), ",
        "unit_dose_qty": ", 20, ",
        "dose_unit_eng": ", Gram, ",
        "freq_time_unit_eng": ", , ",
        "therapy_duration": ", 2, ",
        "therapy_duration_unit_eng": ", Days, ",
        "indication_eng": " , Kawasaki's disease,  ",
        "pt_name_eng": "Coombs direct test positive, Haemoglobin decreased",
        "meddra_version": "v.27.1, v.27.1",
        "duration": ", ",
        "duration_unit_eng": ", "
    }
]


def split_comma_values(value):
    """Helper function to split comma-separated values into an array and remove placeholders."""
    # Split and strip any spaces, also remove placeholder values
    placeholders = ["{{health_product_role}}", "{{dosage_form}}", "{{route_of_administration}}",
                    "{{dose}}", "{{frequency}}", "{{therapy_duration}}", "{{indication}}",
                    "{{meddra_version}}", "{{reaction_duration}}"]
    values = [v.strip() for v in value.split(',') if v.strip() not in placeholders]
    return values


def format_combined_values(quantity, unit):
    """Combine quantity and unit into a single string."""
    return f"{quantity} {unit}" if quantity and unit else ""


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
    fields['therapy_duration'] = [format_combined_values(dur, unit) for dur, unit in zip(fields['therapy_duration'], fields['therapy_unit'])]

    return fields


def generate_html(data):
    """Generate the full HTML content from the data."""
    item = data[0]
    formatted_data = format_data(item)

    # Read the template HTML and inject the generated content
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
    html_template = html_template.replace('{{serious}}', item.get('seriousness_eng', ''))

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
    for i in range(len(formatted_data['drug_name'])):
        product_row_values = [
            formatted_data['drug_name'][i], formatted_data['drug_involvement'][i], formatted_data['dosage_form'][i],
            formatted_data['route'][i], formatted_data['dose'][i], formatted_data['freq_time'][i],
            formatted_data['therapy_duration'][i], formatted_data['indication'][i]
        ]

        # Add product row if there are non-empty values
        product_rows += f"<tr><td>{formatted_data['drug_name'][i]}</td><td>{formatted_data['drug_involvement'][i]}</td><td>{formatted_data['dosage_form'][i]}</td><td>{formatted_data['route'][i]}</td><td>{formatted_data['dose'][i]}</td><td>{formatted_data['freq_time'][i]}</td><td>{formatted_data['therapy_duration'][i]}</td><td>{formatted_data['indication'][i]}</td></tr>"

    # Only insert rows if there are valid product rows
    if not product_rows.strip():
        product_rows = "<tr><td colspan='8'>No product data available</td></tr>"

    # Insert product rows into the product info section
    html_template = html_template.replace('{{product_description}}', product_rows)

    # Skip placeholders and empty values in the adverse reaction table
    adverse_reaction_rows = ""
    for i in range(len(formatted_data['pt_name'])):
        adverse_reaction_values = [
            formatted_data['pt_name'][i], formatted_data['meddra_version'][i], formatted_data['duration'][i],
            formatted_data['duration_unit'][i]
        ]

        if any(v.strip() not in ["", "{{meddra_version}}", "{{reaction_duration}}"] for v in adverse_reaction_values):
            adverse_reaction_rows += f"<tr><td>{formatted_data['pt_name'][i]}</td><td>{formatted_data['meddra_version'][i]}</td><td>{formatted_data['duration'][i]} {formatted_data['duration_unit'][i]}</td></tr>"

    if not adverse_reaction_rows.strip():
        adverse_reaction_rows = "<tr><td colspan='3'>No adverse reaction data available</td></tr>"

    html_template = html_template.replace('{{adverse_reaction_terms}}', adverse_reaction_rows)

    # Remove unnecessary placeholders from the HTML template
    placeholders_to_remove = [
        "{{health_product_role}}", "{{dosage_form}}", "{{route_of_administration}}",
        "{{dose}}", "{{frequency}}", "{{therapy_duration}}", "{{indication}}",
        "{{meddra_version}}", "{{reaction_duration}}"
    ]
    for placeholder in placeholders_to_remove:
        html_template = html_template.replace(placeholder, '')

    # Return the final HTML content
    return html_template



def upload_html_to_s3(html_content):
    """Upload the generated HTML to an S3 bucket."""
    # Initialize boto3 client
    s3_client = boto3.client('s3')

    # Specify the file name and S3 bucket info
    file_name = 'input-html/input.html'
    bucket_name = 'cvp-2-bucket'

    # Upload the HTML file to the S3 bucket
    s3_client.put_object(Body=html_content, Bucket=bucket_name, Key=file_name)


# Calling the functions
html_content = generate_html(json_data)
upload_html_to_s3(html_content)
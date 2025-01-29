import requests
import base64
from datetime import datetime

# Soda Cloud API credentials
API_KEY_ID = "61e128b4-9063-4d32-9e7e-0dd01566f5fb"
API_KEY_SECRET = "6TFKQOJD2UoeyUKMT9_yn9V2X5PgiYcpVhF_jRpdVcR-5NcFis1yEQ"

# Soda Cloud Host
HOST = "https://cloud.us.soda.io"

def get_soda_data_paginated(datasets):
    """Fetch all datasets information from Soda Cloud, handling pagination."""
    auth_string = base64.b64encode(f"{API_KEY_ID}:{API_KEY_SECRET}".encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {auth_string}"}
    
    all_datasets = []
    page_number = 0
    page_size = 10  # Adjust if needed

    while True:
        response = requests.get(f"{HOST}/api/v1/{datasets}?page={page_number}&size={page_size}", headers=headers)

        if response.status_code == 200:
            data = response.json()
            if 'content' in data:
                all_datasets.extend(data['content'])
                if data['last']:
                    break
                page_number += 1
            else:
                break
        else:
            print(f"Error: {response.status_code} - {response.json()}")
            break
    
    return all_datasets

def check_specific_dataset_updated_on_date(datasets, dataset_name, specific_date):
    """Check if a specific dataset has been updated on a specific date."""
    for dataset in datasets:
        if dataset['name'].lower() == dataset_name.lower():
            last_updated_str = dataset.get('lastUpdated')
            if last_updated_str:
                last_updated = datetime.fromisoformat(last_updated_str.rstrip('Z'))
                # Compare the date (ignore time)
                return last_updated.date() == specific_date.date()
    return False

# Get data about all datasets with pagination support
all_datasets = get_soda_data_paginated("datasets")

if all_datasets:
    # Specify the dataset name and the date to check
    specific_dataset_name = "YourDatasetNameHere"  # Replace with your dataset name
    specific_date = datetime(2024, 9, 22)  # Replace with the specific date you want to check

    # Check if the specific dataset was updated on the specific date
    if check_specific_dataset_updated_on_date(all_datasets, specific_dataset_name, specific_date):
        print(f"The dataset '{specific_dataset_name}' was updated on {specific_date.date()}.")
    else:
        print(f"The dataset '{specific_dataset_name}' was not updated on {specific_date.date()}.")
else:
    print("Failed to retrieve datasets or no datasets available.")

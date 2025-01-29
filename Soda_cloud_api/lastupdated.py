import requests
import base64
from datetime import datetime, timedelta

# Soda Cloud API credentials
API_KEY_ID = "61e128b4-9063-4d32-9e7e-0dd01566f5fb"
API_KEY_SECRET = "6TFKQOJD2UoeyUKMT9_yn9V2X5PgiYcpVhF_jRpdVcR-5NcFis1yEQ"

# Soda Cloud Host
HOST = "https://cloud.us.soda.io"

def get_soda_data_paginated(datasets):
    """Fetch all datasets information from Soda Cloud, handling pagination."""
    # Generate Basic Auth Header
    auth_string = base64.b64encode(f"{API_KEY_ID}:{API_KEY_SECRET}".encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {auth_string}"}
    
    all_datasets = []
    page_number = 0
    page_size = 10  # Adjust if needed

    while True:
        # Send the API request for each page
        response = requests.get(f"{HOST}/api/v1/{datasets}?page={page_number}&size={page_size}", headers=headers)

        # Check response status and handle errors
        if response.status_code == 200:
            data = response.json()
            if 'content' in data:
                all_datasets.extend(data['conten'])
                # Check if there are more pages
                if data['last']:
                    break
                page_number += 1
            else:
                break
        else:
            print(f"Error: {response.status_code} - {response.json()}")
            break
    
    return all_datasets

def filter_datasets_by_datasource(datasets, datasource_name):
    """Filter datasets by the specified data source name."""
    filtered_datasets = [dataset for dataset in datasets if dataset['datasource']['name'].lower() == datasource_name.lower()]
    return filtered_datasets

def filter_datasets_by_last_24_hours(datasets):
    """Filter datasets updated in the last 24 hours."""
    filtered_datasets = []
    last_24_hours = datetime.now() - timedelta(hours=24)
    
    for dataset in datasets:
        # Assuming dataset has a 'lastUpdated' field with ISO 8601 date format
        last_updated_str = dataset.get('lastUpdated')
        if last_updated_str:
            last_updated = datetime.fromisoformat(last_updated_str.rstrip('Z'))  # Strip the 'Z' if present for UTC
            if last_updated >= last_24_hours:
                filtered_datasets.append(dataset)

    return filtered_datasets

# Get data about all datasets with pagination support
all_datasets = get_soda_data_paginated("datasets")

if all_datasets:
    # Filter datasets by the data source name "SodaDemo"
    filtered_datasets = filter_datasets_by_datasource(all_datasets, "SodaDemo")

    # Further filter datasets by the ones updated in the last 24 hours
    recent_datasets = filter_datasets_by_last_24_hours(filtered_datasets)

    # Print the filtered datasets
    if recent_datasets:
        print(f"Datasets for datasource 'SodaDemo' updated in the last 24 hours:")
        for dataset in recent_datasets:
            print(f"- {dataset['name']} (ID: {dataset['id']}, Health Status: {dataset['healthStatus']}, Data Quality Status: {dataset['dataQualityStatus']})")
    else:
        print("No datasets found for the specified datasource that were updated in the last 24 hours.")
else:
    print("Failed to retrieve datasets or no datasets available.")

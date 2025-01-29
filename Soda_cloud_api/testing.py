"Datasource name for SodaDemo"

import requests
import base64

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
                all_datasets.extend(data['content'])
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
    # Filter datasets by the given data source name
    filtered_datasets = [dataset for dataset in datasets if dataset['datasource']['name'].lower() == datasource_name.lower()]
    return filtered_datasets

# Get data about all datasets with pagination support
all_datasets = get_soda_data_paginated("datasets")

if all_datasets:
    # Filter datasets by the data source name "SodaDemo"
    filtered_datasets = filter_datasets_by_datasource(all_datasets, "SodaDemo")
    
    # Print the filtered datasets
    if filtered_datasets:
        print(f"Datasets for datasource 'SodaDemo':")
        for dataset in filtered_datasets:
            print(f"- {dataset['name']} (ID: {dataset['id']}, Health Status: {dataset['healthStatus']}, Data Quality Status: {dataset['dataQualityStatus']})")
    else:
        print("No datasets found for the specified datasource.")
else:
    print("Failed to retrieve datasets or no datasets available.")
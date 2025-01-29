import requests
import base64

# Updated Soda Cloud API credentials
API_KEY_ID = "61e128b4-9063-4d32-9e7e-0dd01566f5fb"
API_KEY_SECRET = "6TFKQOJD2UoeyUKMT9_yn9V2X5PgiYcpVhF_jRpdVcR-5NcFis1yEQ"

# Update the host
HOST = "https://cloud.us.soda.io"

def get_soda_data(datasets):
    # Generate Basic Auth Header
    auth_string = base64.b64encode(f"{API_KEY_ID}:{API_KEY_SECRET}".encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {auth_string}"}

    # Send the API request
    response = requests.get(f"{HOST}/api/v1/{datasets}", headers=headers)
    
    # Check response status and handle errors
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.json()}")
        return None

# Get data about all datasets
datasets_data = get_soda_data("datasets")

if datasets_data:
    print(datasets_data)

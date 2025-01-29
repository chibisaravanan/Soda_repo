import requests
import base64
import snowflake.connector

# Replace with your Soda Cloud API credentials
API_KEY_ID = "61e128b4-9063-4d32-9e7e-0dd01566f5fb"
API_KEY_SECRET = "6TFKQOJD2UoeyUKMT9_yn9V2X5PgiYcpVhF_jRpdVcR-5NcFis1yEQ"

# Updated Snowflake credentials
username = "COE_DA_USER"
password = "Da_snow@2024"
account = "kh24787.ap-south-1.aws"
database = "DATA_ASSURANCE"
schema = "RETAIL_PROD"
warehouse = "COE_DQ_WH"
role = "COE_DQ_LABS"

def get_soda_data(datasets):
    """Fetches data from Soda Cloud API."""
    auth_string = base64.b64encode(f"{API_KEY_ID}:{API_KEY_SECRET}".encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {auth_string}"}
    response = requests.get(f"https://cloud.us.soda.io/api/v1/{datasets}", headers=headers)
    
    # Check if the response is successful
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data from Soda Cloud API. Status code: {response.status_code}, Response: {response.text}")
        return None

def store_data_in_snowflake(data):
    if not data:
        print("No data to store in Snowflake.")
        return

    # Print the entire data response to understand its structure
    print("Data response from Soda Cloud API:", data)

    # Check if the 'content' key exists in the data
    if 'content' not in data:
        print(f"Unexpected data format: {data}")
        return

    # Continue with Snowflake data insertion process
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        client_session_keep_alive=True,
        session_parameters={
            'QUERY_TAG': 'soda-queries',
            'QUOTED_IDENTIFIERS_IGNORE_CASE': False
        }
    )
    cur = conn.cursor()

    # Set the database and schema context explicitly
    cur.execute(f"USE DATABASE {database};")
    cur.execute(f"USE SCHEMA {schema};")

    # Define the fully qualified table name
    table_name = f"{schema}.check_results"

    # Create the table with specific columns: check name, result (status), and timestamp
    create_table_query = f"""
    CREATE OR REPLACE TABLE {table_name} (
        check_name STRING,
        status STRING,
        timestamp TIMESTAMP
    );
    """
    cur.execute(create_table_query)

    # Prepare the insert query
    insert_query = f"INSERT INTO {table_name} (check_name, status, timestamp) VALUES (%s, %s, %s);"

    # Extract the required fields and insert them
    for record in data['content']:
        check_name = record.get('name', 'Unknown')
        status = record.get('dataQualityStatus', 'Unknown')
        timestamp = record.get('lastUpdated')

        # Insert data into Snowflake
        cur.execute(insert_query, (check_name, status, timestamp))

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Selected data successfully stored in Snowflake!")

# Fetch data from Soda Cloud API
datasets_data = get_soda_data("datasets")

# Store the selected data in Snowflake
store_data_in_snowflake(datasets_data)

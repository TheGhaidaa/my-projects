import os
import boto3
import requests
import snowflake.connector as sf


# A function for Loading the csv file from S3 into Lambda
def data_upload(file):
    # Download the data from the API endpoint
    response = requests.get(file['url'])
    response.raise_for_status()

    # Save the data to the destination file in /tmp directory
    file_path = os.path.join(file['destination_folder'], file['file_name'])
    with open(file_path, 'wb') as file:
        file.write(response.content)
        
    with open(file_path, 'r') as file:
        file_content = file.read()
        print("File Content:")
        print(file_content)
    
 
# This function for establishing the Snowflake connection, and returning conn and cursor to be used in queries_execution()
def snowflake_connection(snow):
    user = snow['user']
    password = snow['password']
    account = snow['account']
    warehouse = snow['warehouse']
    database = snow['database']
    schema = snow['schema']
    role = snow['role']

    conn = sf.connect(user = user, password = password, \
                 account = account, warehouse = warehouse, \
                 database = database,  schema = schema,  role = role)

    cursor = conn.cursor()
    return conn, cursor
    

# Saving all queries in a list to send it to queries_execution() to be executed
def snowflake_queries(snow, file):
    
    queries = []
    
    # required information in this function
    warehouse = snow['warehouse']
    schema = snow['schema']
    table = snow['table']
    stage_name = snow['stage_name']
    file_name = file['file_name']
    local_file_path = file['local_file_path']


    # use warehouse
    use_warehouse = f"use warehouse {warehouse};"
    queries.append(use_warehouse)

    # use schema
    use_schema = f"use schema {schema};"
    queries.append(use_schema)
    
    # create CSV format
    create_csv_format = f"CREATE or REPLACE FILE FORMAT COMMA_CSV TYPE ='CSV' FIELD_DELIMITER = ',';"
    queries.append(create_csv_format)
    
    # create stage
    create_stage_query = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT =COMMA_CSV;"
    queries.append(create_stage_query)

    # Copy the file from local to the stage
    copy_into_stage_query = f"PUT 'file://{local_file_path}' @{stage_name};"
    queries.append(copy_into_stage_query)
    
    # List the stage
    list_stage_query = f"LIST @{stage_name};"
    queries.append(list_stage_query)
    
    # truncate table
    truncate_table = f"truncate table {schema}.{table};"  
    queries.append(truncate_table)    

    # Load the data from the stage into a table
    copy_into_query = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT =COMMA_CSV on_error='continue';"  
    queries.append(copy_into_query)
    
    return queries


# Executing queries to upload the 'inventory.csv' file to the Snowflake table
def queries_execution(conn, cursor, queries):
    for query in queries:
        cursor.execute(query)
        conn.commit()


def lambda_handler(event, context):

    # 1- setting the needed inforamtion into dictionary
    file_info = {
        'url': 'https://de-materials-tpcds.s3.ca-central-1.amazonaws.com/inventory.csv',
        'destination_folder': '/tmp',
        'file_name': 'inventory.csv',
        'local_file_path': '/tmp/inventory.csv' 
    }
    
    snowflake_parameters = {
        'account':'MWUGXJK-ARB92400',
        'warehouse': 'TPCDS',
        'database': 'TPCDS',
        'schema': 'RAW',
        'table': 'inventory',
        'user': 'wcdsnow',
        'password': 'Wcd123456',
        'role': 'accountadmin',
        'stage_name': 'inv_Stage'
    }
 
    # 2- Load csv file from S3 into Lambda
    data_upload(file_info)
    
    # 3- Establish Snowflake connection
    conn, cursor = snowflake_connection(snowflake_parameters)
    
    # 4- Upload the csv file into Snowflake table
    queries = snowflake_queries(snowflake_parameters, file_info)
    queries_execution(conn, cursor, queries)
    

    return {
        'statusCode': 200,
        'body': 'File downloaded and uploaded to Snowflake successfully.'
    }

import mysql.connector
import boto3
from botocore.exceptions import NoCredentialsError
import os
from datetime import datetime,date


def retrieve_secret(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
        
        # Extract the secret string from the response
        secret = response['SecretString']
        return secret
        
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        return None

secret_name = 'webapp'
retrieved_secret = eval(retrieve_secret(secret_name))
mysql_password = retrieved_secret['MysqlPassword']
auth_key = retrieved_secret['AuthKey']


# MySQL database connection configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': mysql_password,
    'database': 'jobs'
}


current_dt = (datetime.utcnow()).date()
# S3 bucket and object details
s3_bucket_name = 'data-programming-bdat1004-web-app'
output_file_name = f'temp/jobs_export/dt={current_dt}/export.csv'  # Change this as per your requirement

# Establish MySQL connection
try:
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # MySQL query to select data from table
    query = "SELECT * FROM jobs"
    cursor.execute(query)

    # Fetch data from MySQL
    data = cursor.fetchall()


    # Write data to a CSV file
    with open('temp.csv', 'w') as file:
        for row in data:
            file.write(','.join(map(str, row)) + '\n')

    # Upload the file to Amazon S3
    s3 = boto3.client('s3')
    s3.upload_file('temp.csv', s3_bucket_name, output_file_name)

    print(f"Data successfully exported from MySQL table and uploaded to S3 bucket: {s3_bucket_name}/{output_file_name}")

except mysql.connector.Error as error:
    print(f"Error while connecting to MySQL: {error}")

except NoCredentialsError:
    print("AWS credentials not found or incorrect. Make sure your AWS credentials are configured correctly.")
    
finally:
    # Close MySQL connection and remove local file
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
    if 'output_file_name' in locals():
        os.remove('temp.csv')  # Remove the local file after upload

from faker import Faker
import random
import string
import os
import csv

from google.cloud import storage


try:
    SERVICE_ACCOUNT_KEY_PATH = '/opt/airflow/cred/gcp-test.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY_PATH
    print(f"Service account key path set to: {SERVICE_ACCOUNT_KEY_PATH}")
except Exception as e:
    print(f"An error occurred while setting the service account key path: {e}")


# Specify number of employees to generate
num_employees = 100

# Create Faker instance
fake = Faker()

employee_data= []

# Define the character set for the password
password_characters = string.ascii_letters + string.digits + 'm'

# Generate employee data and save it to a CSV file
with open('sample_test_1.csv', mode='w', newline='') as file:
    fieldnames = ['first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    for _ in range(num_employees):
        writer.writerow({
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "job_title": fake.job(),
            "department": fake.job(),  # Generate department-like data using the job() method
            "email": fake.email(),
            "address": fake.city(),
            "phone_number": fake.phone_number(),
            "salary": fake.random_number(digits=5),  # Generate a random 5-digit salary
            "password": ''.join(random.choice(password_characters) for _ in range(8))  # Generate an 8-character password with 'm'
        })

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client= storage.Client()
    bucket= storage_client.bucket(bucket_name)
    blob= bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}')


# GCS bucket , source & destination file name
bucket_name= 'sn_insights_test'
source_file_name= 'sample_test_1.csv'
destination_blob_name = 'etl_df_af/sample_test_1.csv'

upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
from faker import Faker
import random
import string

from google.cloud import storage

# Specify number of employees to generate
num_employees = 10

# Create Faker instance
fake = Faker()

employee_data= []

# Define the character set for the password
password_characters = string.ascii_letters + string.digits + 'm'

for _ in range(num_employees):
    employee={
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "job_title": fake.job(),
        "department": fake.job(),
        "email": fake.email(),
        "address": fake.address(),
        "phone_number": fake.phone_number(),
        "salary": fake.random_number(digits=5),
        "password": "".join(random.choice(password_characters) for _ in range(8))
    }

#     employee_data.append(employee)

# print(employee_data)

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
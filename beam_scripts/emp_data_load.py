import apache_beam as beam
from apache_beam.io.textio import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
import time
import os
import csv  # Ensure this import is present
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound


try:
    SERVICE_ACCOUNT_KEY_PATH = '/opt/airflow/cred/gcp-test.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY_PATH
    print(f"Service account key path set to: {SERVICE_ACCOUNT_KEY_PATH}")
except Exception as e:
    print(f"An error occurred while setting the service account key path: {e}")



# Define the schema for the BigQuery table
table_schema = {
    'fields': [
        {'name': 'first_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'job_title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'department', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'address', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'salary', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'password', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'source_file_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'processing_timestamp', 'type': 'TIMESTAMP'}
    ]
}

def transform_load(file_name):
    project_id = 'aceinternal-2ed449d3'
    dataset_id = 'sandbox_ss_eu'
    table_id = 'sample_test_1'

    options = PipelineOptions([
        '--project=' + project_id,
        '--job_name=etl-df-af-1',
        '--staging_location=gs://sn_insights_test/staging',
        '--temp_location=gs://sn_insights_test/temp',
        # '--runner=DataflowRunner',
        '--region=europe-west2',
    ])

    from datetime import datetime  # Add this import

    def format_for_bigquery_output(kv, file_name):
        # Check if the row has the correct number of columns
        if len(kv) != 9:
            raise ValueError(f"Invalid data format: {kv}")
        return {
            'first_name': kv[0],
            'last_name': kv[1],
            'job_title': kv[2],
            'department': kv[3],
            'email': kv[4],
            'address': kv[5],
            'phone_number': kv[6],
            'salary': int(kv[7]),  # Ensure salary is an integer
            'password': kv[8],
            'source_file_name': os.path.basename(file_name),
            # Use datetime.utcnow() for processing timestamp
            "processing_timestamp": datetime.utcnow().isoformat() + "Z"
        }

    import csv
    def decode_csv(line):
        import csv
        # Use csv.reader to properly parse CSV lines
        return next(csv.reader([line]))

    with beam.Pipeline(options=options) as pipe1:
        (
            pipe1
            | "ReadFromText" >> ReadFromText(file_name, skip_header_lines=1)
            | "DecodeCSV" >> beam.Map(decode_csv)
            | "FormatForBigQueryOutput" >> beam.Map(lambda kv: format_for_bigquery_output(kv, file_name))
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=f"{project_id}:{dataset_id}.{table_id}",
                schema=table_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED)
        )

def hello_pubsub():
    project_id = "aceinternal-2ed449d3"
    dataset_id = "sandbox_ss_eu"
    table_id = "sample_test_1"

    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

    client = storage.Client(project=project_id)
    bucket_name = "sn_insights_test"
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix="etl_df_af/")

    bq_client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_ref} exists.")
    except NotFound:
        print(f"Table {table_ref} not found, creating it...")
        table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=table_schema['fields'])
        bq_client.create_table(table)
        print(f"Table {table_ref} created.")

    c = 0
    for blob in blobs:
        file_name = blob.name
        file_name_base = os.path.basename(file_name)

        query = f"SELECT COUNT(1) FROM `{project_id}.{dataset_id}.{table_id}` WHERE source_file_name = '{file_name_base}'"
        result = bq_client.query(query).result()

        if list(result)[0][0] == 0:
            transform_load(f'gs://{bucket_name}/{file_name}')
            c += 1

    if c >= 1:
        print(f'New {c} files appended')
    else:
        print('No files appended')

if __name__ == '__main__':
    hello_pubsub()
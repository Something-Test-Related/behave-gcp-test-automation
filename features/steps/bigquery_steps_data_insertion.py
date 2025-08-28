from behave import *
from google.cloud import bigquery
from logging import getLogger
import csv
import tempfile

log = getLogger("bigquery_steps_data_insertion")


# Insert data from a given CSV file into the specified BigQuery table
@step('the data in CSV "{csv_path}" has been inserted into table "{table_name}"')
@step('the data in CSV "{csv_path}" has been inserted into table "{table_name}" with null_marker "{null_marker}"')
def step_impl(context, csv_path, table_name, null_marker="NULL"):

    # Load job config
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        null_marker=null_marker.strip()
    )

    # Make sure table exists and fetch row count 
    destination_table = context.bigquery.get_table(table_name) 
    before_rows = destination_table.num_rows

    # Load data
    with open(csv_path, 'rb') as file:
        load_job = context.bigquery.load_table_from_file(file, table_name, job_config=job_config)
        load_job.result()  # Wait for job to complete

    # Log inserted rows 
    destination_table = context.bigquery.get_table(table_name) 
    inserted = destination_table.num_rows - before_rows 
    log.debug(f"{inserted} rows inserted into table {table_name}.")


# Insert data from a given SQL file into the specified BigQuery table
@step('the data in the SQL insert file "{sql_path}" has been inserted into table "{table_name}"')
def step_impl(context, sql_path, table_name):

    # Replace the table_id placeholder in the file with the table_name passed in the step
    sql_file = open(sql_path)
    insert_query = sql_file.read().replace('table_id', table_name)
    sql_file.close()

    # Execute the insert query
    query_job = context.bigquery.query(insert_query)
    query_job.result()


# Insert data from a Gherkin table into Bigquery
@step('the gherkin table of data has been inserted into BigQuery table "{table_name}"')
def step_impl(context, table_name):
    if not context.table:
        raise Exception("This step requires a Gherkin table.")

    # Write Gherkin table to a temp CSV
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False) as temp_csv:
        writer = csv.writer(temp_csv)
        writer.writerow(context.table.headings)
        for row in context.table.rows:
            writer.writerow(["" if v == "NULL" else v for v in row])
        temp_csv_path = temp_csv.name

    # Load job config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        null_marker="",  
    )

    # Load the data from the temp CSV into BigQuery
    with open(temp_csv_path, "rb") as file:
        load_job = context.bigquery.load_table_from_file(file, table_name, job_config=job_config)
        load_job.result()  # Wait for completion

    if load_job.errors:
        raise RuntimeError(f"BigQuery load job failed: {load_job.errors}")

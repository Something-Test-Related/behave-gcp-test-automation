from behave import *
from io import StringIO


def open_storage_csv(context, csv_path, bucket_name):
    # Select the bucket and get the CSV from within the bucket as a string
    bucket = context.storage.bucket(bucket_name)
    blob = bucket.blob(csv_path)
    blob_string = blob.download_as_string()
    blob_string = blob_string.decode('utf-8')
    blob_string = StringIO(blob_string)
    
    return blob_string


# Check a bucket with the given name exists
@step('a bucket called "{bucket_name}" exists')
@step('a bucket called "{bucket_name}" should exist')
def step_impl(context, bucket_name):
    matching_bucket = next((bucket for bucket in context.storage.list_buckets() if bucket.name == bucket_name), None)

    if not matching_bucket:
        raise Exception(f"No bucket with name {bucket_name} exists")


# Check a file exists in the given path
@step('a file called "{file_name}" exists at path "{path}"')
@step('a file called "{file_name}" should exist at path "{path}"')
def step_impl(context, file_name, path):
    split_path = path.split("/", 1)
    bucket_name = split_path[0]

    # Select the bucket and get a list of all paths found within the bucket
    bucket = context.storage.bucket(bucket_name)
    blobs = context.storage.list_blobs(bucket)

    # Filter the list of all paths in the bucket to only full file paths 
    found_files = []
    for blob in blobs: 
        if not blob.name.endswith("/"):
            found_files.append(blob.name)
    
    # Check if the given file and path exists in the filtered list
    if not f"{split_path[1]}{file_name}" in found_files:
        raise AssertionError(f"No file with path {split_path[1]}{file_name}")


# Verify the contents of a CSV file at the given path
@given('the file called "{file_name}" at path "{path}" contains the following data')
def step_impl(context, file_name, path):

    # Gherkin table detailing expected data required
    if not context.table:
        raise Exception("this step requires a Gherkin table detailing the expected headings and values")
    
    # Join to create the full path then split out the bucket name from the rest of the path
    full_path = f"{path}{file_name}"
    split_path = full_path.split("/", 1) 

    # Read CSV from storage into an array of lines 
    csv = open_storage_csv(context, split_path[1], split_path[0])
    csv_lines = [line.strip() for line in csv]

    # Read Gherkin table line by line and ensure the row exists in the CSV
    # This is not a forgiving or thorough way of doing this comparison. A row can exist multiple times or will not match if the table order is incorrect
    for row in context.table.rows:
        row_line = ",".join(row).strip()

        if row_line not in csv_lines: 
            raise AssertionError(f"The following row could not be found in the CSV: {row_line}")
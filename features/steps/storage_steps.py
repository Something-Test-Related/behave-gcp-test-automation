from behave import *


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
        raise Exception(f"No file with path {split_path[1]}{file_name}")

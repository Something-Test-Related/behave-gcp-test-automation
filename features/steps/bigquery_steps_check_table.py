from behave import *


# Check table with the given name exists
@step('a table called "{table_name}" exists')
@step('a table called "{table_name}" should exist')
def step_impl(context, table_name):

    table = context.bigquery.get_table(table_name)

    if not table: 
        raise Exception(f"No table with name {table_name} exists")


# Check the table in BigQuery has the expected column names and data types
@then('the table "{table_name}" should contain the following structure')
def step_impl(context, table_name):

    # Gherkin table detailing column names and types required
    if not context.table:
        raise ValueError(
            "this step requires a Gherkin table detailing column names and types")

    table_path = table_name.split(".")

    structure_query = """
    SELECT field_path, data_type
    FROM {dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE table_name = "{table}"
    """.format(dataset=table_path[0],table=table_path[1]
    )

    query_job = context.bigquery.query(structure_query)
    result = query_job.result()

    # Create tuple lists of column names and types for what actually exists in BQ and what we expect to exist
    actual_structure = {(row[0], row[1]) for row in result}
    expected_structure = {(row[0], row[1]) for row in context.table.rows}

    # Check all expected column names and types are present in the BQ table
    missing_columns = expected_structure - actual_structure
    if missing_columns:
        raise Exception(
            f"Expected the following column(s) to exist in {table_name}: {', '.join(f'{col[0]} ({col[1]})' for col in missing_columns)}")


# Check the table in BigQuery is partitioned as expected
@then('the table "{table_name}" should be partitioned by column "{partition}"')
def step_impl(context, table_name, partition):
    # Split the table_name to get dataset and table
    table_path = table_name.split(".")

    expected_partition = partition.strip()

    # BigQuery query to get partitioning column
    partition_query = """
    SELECT column_name
    FROM {dataset}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name="{table}"
    AND is_partitioning_column = "YES"
    """.format(dataset=table_path[0], table=table_path[1])

    # Execute the query
    query_job = context.bigquery.query(partition_query)
    results = query_job.result()

    # Extract actual partition column from BigQuery result
    row = next(results, None)
    actual_partition = row['column_name'].strip() if row else None

    # Check if the expected partition column exists in the actual partitions
    if expected_partition != actual_partition:
        raise AssertionError(
            f"Expected partition column {expected_partition} to exist in table {table_name}, but found: {actual_partition}")


# Check the table in BigQuery is not partitioned
@then('the table "{table_name}" should not be partitioned')
def step_impl(context, table_name):
    table_path = table_name.split(".")

    partition_query = """
    SELECT column_name
    FROM {DATASET}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name="{TABLE}"
    AND is_partitioning_column = "YES"
    """.format(DATASET=table_path[0], TABLE=table_path[1])

    query_job = context.bigquery.query(partition_query)
    results = query_job.result()
    actual_partitions = [row['column_name'] for row in results]

    if len(actual_partitions) > 0:
        raise Exception(
            f"Expected no partitions to exist in table {table_name} but found partition: {actual_partitions[0]}")
    

# Check the table in BigQuery has the expected row count
@given('table "{table_name}" contains "{row_count}" rows')
@then('table "{table_name}" should contain "{row_count}" rows')
def step_impl(context, table_name, row_count):

    row_count_query = """
    SELECT COUNT(*) FROM {TABLE_NAME} t
    """.format(TABLE_NAME=table_name)

    query_job = context.bigquery.query(row_count_query)
    results = query_job.result()

    for result in results:
        actual_row_count = result.values()[0]

    if(actual_row_count != int(row_count)):
        raise Exception(
            f"Expected {table_name} to have {row_count} rows, but found {actual_row_count} rows")
from behave import *
from decimal import Decimal, ROUND_HALF_UP


# Check table with the given name exists
@step('a table called "{table_name}" exists')
@step('a table called "{table_name}" should exist')
def step_impl(context, table_name):

    table = context.bigquery.get_table(table_name)

    if not table: 
        raise Exception(f"No table with name {table_name} exists")


# Check the table in BigQuery has the expected column names and data types
@step('the table "{table_name}" contains the following structure')
@then('the table "{table_name}" should contain the following structure')
def step_impl(context, table_name):

    # Gherkin table detailing column names and types required
    if not context.table:
        raise Exception("this step requires a Gherkin table detailing column names and types")

    table_path = table_name.split(".")

    structure_query = """
    SELECT field_path, data_type
    FROM {DATASET}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE table_name = "{TABLE}"
    """.format(DATASET=table_path[0],TABLE=table_path[1])

    query_job = context.bigquery.query(structure_query)
    result = query_job.result()

    # Create tuple lists of column names and types for what actually exists in BQ and what we expect to exist
    actual_structure = {(row[0], row[1]) for row in result}
    expected_structure = {(row[0], row[1]) for row in context.table.rows}

    # Check all expected column names and types are present in the BQ table
    missing_columns = expected_structure - actual_structure
    if missing_columns:
        raise AssertionError(
            f"Expected the following column(s) to exist in {table_name}: {', '.join(f'{col[0]} ({col[1]})' for col in missing_columns)}")


# Check the table in BigQuery is partitioned as expected
@step('the table "{table_name}" is partitioned by column "{partition}"')
@then('the table "{table_name}" should be partitioned by column "{partition}"')
def step_impl(context, table_name, partition):
    # Split the table_name to get dataset and table
    table_path = table_name.split(".")

    expected_partition = partition.strip()

    # Query to get partitioning column
    partition_query = """
    SELECT column_name
    FROM {DATASET}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name= "{TABLE}"
    AND is_partitioning_column = "YES"
    """.format(DATASET=table_path[0], TABLE=table_path[1])

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
@step('the table "{table_name}" is not partitioned')
@then('the table "{table_name}" should not be partitioned')
def step_impl(context, table_name):
    table_path = table_name.split(".")

    partition_query = """
    SELECT column_name
    FROM {DATASET}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = "{TABLE}"
    AND is_partitioning_column = "YES"
    """.format(DATASET=table_path[0], TABLE=table_path[1])

    query_job = context.bigquery.query(partition_query)
    results = query_job.result()
    actual_partitions = [row['column_name'] for row in results]

    if len(actual_partitions) > 0:
        raise AssertionError(
            f"Expected no partitions to exist in table {table_name} but found partition: {actual_partitions[0]}")
    

# Check the table in BigQuery has the expected row count
@given('table "{table_name}" contains "{row_count}" rows')
@then('table "{table_name}" should contain "{row_count}" rows')
def step_impl(context, table_name, row_count):

    row_count_query = """
    SELECT COUNT(*) FROM {TABLE} t
    """.format(TABLE=table_name)

    query_job = context.bigquery.query(row_count_query)
    results = query_job.result()

    for result in results:
        actual_row_count = result.values()[0]

    if(actual_row_count != int(row_count)):
        raise AssertionError(
            f"Expected {table_name} to have {row_count} rows, but found {actual_row_count} rows")


# Check that rows exist in the given table with the given values
@then('table "{table_name}" should contain the following data')
def step_impl(context, table_name):

    # Gherkin table detailing row values required 
    if not context.table:
        raise Exception("this step requires a Gherkin table detailing expected row values")
    
    # First row is table headers, convert rows into a 2d array of values 
    headers = context.table.headings
    values = [row.cells for row in context.table.rows]

    for row in values:
        query = "WHERE "

        for idx, header in enumerate(headers):

            try: 
                if(float(row[idx]) == float('0') or float(row[idx])):
                    # Comparing floats handled separately as isnumeric(0) does not behave as expected and floats cannot be in quotes
                    query += f"{header} = {row[idx]} "
            except ValueError:
                if(row[idx].lower() == "null"):
                    # If we're comparing to null use the 'IS' comparator 
                    query += f"{header} IS {row[idx]} "
                elif(row[idx] == "true" or row[idx] == "false"):
                    # If we're comparing to a boolean we do not need the single quotes 
                    query += f"{header} = {row[idx]} "
                elif(row[idx].startswith("{")):
                    # If we're comparing JSON convert it to a string first
                    query += f"TO_JSON_STRING({header}) = '{row[idx]}' "
                elif(not row[idx].isnumeric() and not row[idx].startswith("'")):
                    # If we're comparing non-numeric values ensure they are wrapped in single quotes
                    query += f"{header} = '{row[idx]}' "
                else: 
                    # Otherwise compare with a straight '=' operator and no quotes
                    query += f"{header} = {row[idx]} "

            # If this is not the final value in the row append "AND" to continue the query
            if(idx < len(headers) - 1):
                query += "AND "

        # Create the full SQL query to determine if a row exists with all the given values
        final_query = """
            SELECT COUNT(*) FROM {TABLE} t
            {FILTER_QUERY}
        """.format(TABLE=table_name,FILTER_QUERY=query)

        # Execute the query and check the row count is not 0
        query_job = context.bigquery.query(final_query)
        results = query_job.result()

        if(next(results)[0] < 1):
            raise AssertionError(
                f"Expected table {table_name} to have a row with the following values but one could not be found: {row}")


# Check that there are no duplicate rows in given table excluding JSON, ARRAY and STRUCT datatype columns
@then('there should be no duplicate rows in table "{table_name}"')
def step_impl(context, table_name):
    table_path = table_name.split(".")

    # Get column names and data types
    columns_query = """
        SELECT column_name, data_type
        FROM `{DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = "{TABLE}"
        ORDER BY ordinal_position
    """.format(DATASET=table_path[0], TABLE=table_path[1])

    EXCLUDED_TYPES = {"JSON", "ARRAY", "STRUCT"}

    # Filter out JSON, ARRAY and STRUCT columns as these cannot be grouped
    column_results = context.bigquery.query(columns_query).result()
    column_names = [row.column_name for row in column_results if row.data_type.upper() not in EXCLUDED_TYPES]

    if not column_names:
        raise Exception(f"No columns found in table {table_name}")

    # Build GROUP BY clause
    columns = ", ".join(f"`{name}`" for name in column_names)

    # Check for duplicate rows
    duplicate_query = """
        SELECT COUNT(*) as duplicate_count
        FROM {TABLE}
        GROUP BY {COLUMNS}
        HAVING COUNT(*) > 1
        LIMIT 1
        """.format(TABLE=table_name, COLUMNS=columns)
    
    results = context.bigquery.query(duplicate_query).result()

    if next(results, None):
        raise AssertionError(
            f"Duplicate rows found in table {table_name}")


# Check the sum of all values in a given column of given table are as expected to 2dp
@then('the sum of column "{column}" in table "{table_name}" should equal "{expected_sum}"')
def step_impl(context, column, table_name, expected_sum):

    sum_query = """
    SELECT SUM({COLUMN}) as total_sum
    FROM {TABLE} t
    """.format(COLUMN=column, TABLE=table_name)

    # Execute the query
    query_job = context.bigquery.query(sum_query)
    results = query_job.result()

    # Convert float to string to avoid float precision issues when using Decimal then convert to Decimal with proper rounding
    for result in results:
        actual_sum = Decimal(str(result["total_sum"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # Convert expected value to Decimal and round to 2 decimal places to ensure consistent comparison
    expected_sum = Decimal(expected_sum).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    if actual_sum != expected_sum:
        raise AssertionError(
            f"Expected sum of {column} in {table_name} to be {expected_sum}, but got {actual_sum}")
    

# Check that there are no NULLs in a given column of given table
@then('column "{column}" in table "{table_name}" should not contain any NULL values')
def step_impl(context, column, table_name):

    null_count_query = """
    SELECT COUNT(*) FROM {TABLE} t
    WHERE {COLUMN} IS NULL
    """.format(TABLE=table_name, COLUMN=column)

    query_job = context.bigquery.query(null_count_query)
    results = query_job.result()

    actual_null_count = next(results)[0]

    if(actual_null_count != 0):
        raise AssertionError(
            f"Expected {column} in {table_name} to have no NULL values, but found {actual_null_count} NULLs")
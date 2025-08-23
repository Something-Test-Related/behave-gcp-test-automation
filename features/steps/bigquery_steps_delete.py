from behave import *


# Delete all rows in given BigQuery table
@step('all rows are deleted from table "{table_name}"')
def step_impl(context, table_name):

    delete_query = """
    DELETE FROM {TABLE_NAME} WHERE true;
    """.format(TABLE_NAME=table_name)

    query_job = context.bigquery.query(delete_query)
    query_job.result()


# Delete an existing table from BigQuery
@step('the table "{table_name}" is deleted from Big Query')
def step_impl(context, table_name):
    context.bigquery.delete_table(table_name, not_found_ok=True)
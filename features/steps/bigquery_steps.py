from behave import *

# Check table with the given name exists
@step('a table called "{table_name}" exists')
@step('a table called "{table_name}" should exist')
def step_impl(context, table_name):

    table = context.bigquery.get_table(table_name)

    if not table: 
        raise Exception(f"No table with name {table_name} exists")
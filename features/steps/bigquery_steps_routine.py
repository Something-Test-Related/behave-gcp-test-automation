from behave import *
from datetime import date, timedelta


# Run a stored procedure in BigQuery
@step('the "{routine_id}" routine is invoked in dataset "{dataset_id}"')
@step('the "{routine_id}" routine is invoked in dataset "{dataset_id}" with the following parameters')
def step_impl(context, routine_id, dataset_id):
    parameters = ""

    # If Gherkin table is present merge it into a comma separated list of values converted to given types
    if context.table:
        today = date.today()
        rows = []
   
        for row in context.table.rows: 
            if row['VALUE'].lower() == "yesterday":
                rows.append(f"cast(\'{today - timedelta(days=1)}\' as {row['TYPE']})")
            elif row['VALUE'].lower() == "today": 
                rows.append(f"cast(\'{today}\' as {row['TYPE']})")
            elif row['VALUE'].lower() == "tomorrow":
                rows.append(f"cast(\'{today + timedelta(days=1)}\' as {row['TYPE']})")
            else: 
                rows.append(f"cast(\'{row['VALUE']}\' as {row['TYPE']})")
   
        parameters = ",".join(rows)

    # Call the routine
    call_routine_query = f"CALL {dataset_id}.{routine_id}({parameters});"
    query_job = context.bigquery.query(call_routine_query)
    query_job.result()
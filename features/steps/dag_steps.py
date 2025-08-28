from behave import *
import subprocess, re
from polling import poll


# Execute command as a subprocess
# Do not throw an exception on error if allow_error is passed as True
def run_subprocess(cmd, allow_error=False):
    pipe = subprocess.Popen(args=cmd, stdout=subprocess.PIPE, shell=True)
    out, err = pipe.communicate()

    if not allow_error and (pipe.returncode > 0 or err):
        raise RuntimeError(f"Error while running subprocess command: {cmd} - Error: {err}")
    
    return out.decode('utf-8')


# Verify the given dag file exists in storage
@given('a DAG file called "{dag}" exists in Storage') 
def step_impl(context, dag):

    # Gcloud command to list all dags in storage for the test-env environment
    list_cmd = 'gcloud composer environments storage dags list --environment test-env --location europe-west12 --format="value(name)"'
    result = run_subprocess(list_cmd)
    
    if dag not in result:
        raise AssertionError(f"No DAG file called {dag} could be found")


# Verify the given dag exists in the environment - much slower than verifying it exists in storage
@given('a DAG called "{dag}" exists') 
def step_impl(context, dag):

    # Gcloud command to list all dags in the test-env environment
    list_cmd = "gcloud composer environments run test-env --location europe-west12 dags list"
    result = run_subprocess(list_cmd)

    if dag not in result:
        raise AssertionError(f"No DAG called {dag} could be found")


# Kick off a dag with no parameters and wait for it to finish
@step('a DAG called "{dag}" is run')
def step_impl(context, dag):

    # Trigger the given DAG using gcloud executed as a subprocess
    run_cmd = f"gcloud composer environments run test-env --location europe-west12 dags trigger -- {dag} -o json"
    
    result = run_subprocess(run_cmd)
    run_time = re.search('dag_run_id": "manual__([^"]*)', result)

    if not run_time:
        raise RuntimeError(f"No run time could be determined while triggering DAG {dag}")
    
    # Store the execution time so we can reference this specific run again
    dag_run_time = run_time.group(1)
    context.dag_run_time = dag_run_time

    state_cmd = f"gcloud composer environments run test-env --location europe-west12 dags state -- {dag} {dag_run_time}"
    poll_lambda = lambda: not any(status in run_subprocess(state_cmd) for status in ["queued", "running"])

    # Poll the status of the running DAG until it is no longer marked as queued or running
    # Timeout of 10 minutes 
    poll(poll_lambda, step=5, timeout=600)


# Check the state of a DAG run to ensure it is as expected
@then('the last run of DAG "{dag}" should have state "{expected_state}"')
def step_impl(context, dag, expected_state):

    # Ensure we have a saved dag_run_time before continuing 
    if not context.dag_run_time:
        raise Exception(f"No saved run time exists. Ensure a DAG has been run before checking the status")

    state_cmd = f"gcloud composer environments run test-env --location europe-west12 dags state -- {dag} {context.dag_run_time}"
    state_response = run_subprocess(state_cmd)

    if expected_state not in state_response:
        raise AssertionError(f"Expected {dag} run status to be {expected_state} but found {state_response}")
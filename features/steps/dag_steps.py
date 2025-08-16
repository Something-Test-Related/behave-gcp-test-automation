from behave import *
import subprocess, re


# Execute command as a subprocess
# Do not throw an exception on error if allow_error is passed as True
def run_subprocess(cmd, allow_error=False):
    pipe = subprocess.Popen(args=cmd, stdout=subprocess.PIPE, shell=True)
    out, err = pipe.communicate()

    if not allow_error and (pipe.returncode > 0 or err):
        raise Exception(f"Error while running subprocess command: {cmd} - Error: {err}")
    
    return out.decode('utf-8')


# Kick off a dag with no parameters
@step('a DAG called "{dag}" is run')
def step_impl(context, dag):
    run_cmd = f"gcloud composer environments run test-env --location europe-west12 dags trigger -- {dag} -o json"
    
    result = run_subprocess(run_cmd)
    run_time = re.search('dag_run_id": "manual__([^"]*)', result)

    if not run_time:
        raise Exception(f"No run time could be determined while triggering DAG {dag}")
    
    run_time = run_time.group(1)
    context.dag_run_time = run_time
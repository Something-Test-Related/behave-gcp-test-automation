from behave import fixture, use_fixture
from google.cloud import bigquery, storage
from config import load_config


# Set up the connection to GCP Storage
def fixture_storage(context, **kwargs):
    project_id = context.project_id
    context.storage = storage.Client(project=project_id)


# Set up the connection to GCP Bigquery
def fixture_bq(context, **kwargs):
    project_id = context.project_id
    context.bigquery = bigquery.Client(project=project_id)
    context.bigquery.close()


# Map the tag to its fixture function
FIXTURE_TAGS = [
    ["bq", fixture_bq],
    ["storage", fixture_storage]
]


# Function runs before each tag in a feature file 
def before_tag(context, tag):

    # Before the test runs check that all the fixtures are in place for the specified tags
    fixture = next((ftag for ftag in FIXTURE_TAGS if ftag[0] == tag), None)

    if(fixture and not hasattr(context, tag)):
        use_fixture(fixture[1], context)


# Function runs once on framework execution before any tests are executed 
def before_all(context):

    # Load the gcp config file
    load_config(context)
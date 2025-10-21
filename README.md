# behave-gcp-test-automation
BDD Test Automation for GCP using the Python Behave test framework. 
This framework can currently interact with BigQuery, Storage, and Airflow/DAGs and has example tests for each in the `features` folder. 
This framework can be easily expanded to interact with more GCP service and components by building out the `features/enrivonment.py` file.
  
  
## Prerequisites
The following need to be installed before this framework can run: 
- Python [https://www.python.org/](https://www.python.org/)
- Poetry [https://python-poetry.org/](https://python-poetry.org/)
- gcloud [https://cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install)

and you must have an existing project on GCP to run the tests against  
  
  
## Setup

### Create Virtual Environment (optional)
It is recommended to create a virtual environment to run the tests in, but not required

#### Windows 
```py -m venv env```
```.\env\Scripts\activate.ps1```

#### Mac/Linux
```python -m venv env```
```source env/bin/activate``` 


### Install packages with poetry
```poetry install``` 
  

### Update config
The GCP project ID is read from a config in the root directory called `gcp.config`. Either rename the existing `example.config` or create a new config file containing your project ID under the key `project_id`
  

### Authenticate with GCP
Login to gcloud with your GCP account
```gcloud auth login```
  
  
## Usage 
All tests in the `features` folder can be run using command
```behave``` or ```py -m behave```
  
To run specific feature files by name use the `-i` or `--include` option and either the partial or full file name, e.g. to run `storage.feature` use command 
```behave -i storage```

To run all tests using a specific tag use the `-t` or `--tags` option and the tag name, e.g. to run all tests tagged with the BigQuery `@bq` tag use command 
```behave -t bq`
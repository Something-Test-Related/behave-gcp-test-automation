Feature: Using gcloud interact with DAGs running in Airflow on an environment in Composer


    Scenario: Run a DAG with no parameters which loads data from a CSV into BigQuery, wait for the DAG to finish running then check it ran sucessfully
        Given a DAG called "load_csv" is run
        Then the last run of DAG "load_csv" should have state "success"